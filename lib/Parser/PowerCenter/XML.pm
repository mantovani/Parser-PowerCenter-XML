package Parser::PowerCenter::XML;

use Moose;

our $VERSION = '0.01';

use XML::Twig;
use Data::Dumper;

has 'source' => ( is => 'ro', isa => 'Str', default => 'Source Definition' );
has 'target' => ( is => 'ro', isa => 'Str', default => 'Target Definition' );

has 'parser' => (
    is      => 'ro',
    isa     => 'Object',
    default => sub {
        return XML::Twig->new->parsefile( shift->xml );
    },
    lazy    => 1,
    clearer => 'clean_parser',
);

has 'xml' => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
    trigger  => sub {
        my ( $self, $path ) = @_;
        $self->clean_parser;
        die "file don't exist"
          if !-e $path;
    }
);

has 'get_repository' => (
    is      => 'ro',
    isa     => 'Str',
    default => sub {
        my $self = shift;
        my ($repository) = $self->parser->findnodes('//REPOSITORY');
        return $repository->{att}->{NAME};
    },
    lazy => 1
);

has 'get_folder' => (
    is      => 'ro',
    isa     => 'Str',
    default => sub {
        my $self = shift;
        my ($folder) = $self->parser->findnodes('//FOLDER');
        return $folder->{att}->{NAME};
    },
    lazy => 1
);

has 'cache' => (
    is      => 'rw',
    isa     => 'HashRef',
    default => sub { {} },
    clearer => 'clean_cache'
);

sub get_maps {
    my $self = shift;
    return $self->parser->findnodes('//MAPPING');
}

sub get_infs {
    my $self = shift;
    my @maps = $self->get_maps;
    my ( $source_type, $target_type ) = ( $self->source, $self->target );
    my @struct;
    foreach my $map (@maps) {
        my $map_name  = $map->{att}->{NAME};
        my $magic_map = $self->transformation_magic($map);
        push @struct, [ $map_name, @{ $self->mapping( $map, $magic_map ) } ];
    }
    return \@struct;
}

sub mapping {
    my ( $self, $map, $magic_map ) = @_;
    my @struct;
    my $source_type = $self->source;
    my @source_cols =
      $map->findnodes(qq{.//CONNECTOR[\@FROMINSTANCETYPE="$source_type"]});
    foreach my $source_col (@source_cols) {
        my ( $source, $target ) = (
            $source_col->{att},
            $self->recursive_mapping( $map, $source_col, $magic_map )
        );
        if ( ref $target ) {
            push @struct,
              {
                source => {
                    map { $_ => $source->{$_} } grep { /^FROM/ } keys %{$source}
                },
                source_infs =>
                  $self->source_struct( $source->{FROMINSTANCE}, $map ),
                target => {
                    map { $_ => $target->{$_} } grep { /^TO/ } keys %{$target}
                },
                target_infs =>
                  $self->target_struct( $target->{TOINSTANCE}, $map )
              };
        }
    }
    return \@struct;
}

sub recursive_mapping {
    my ( $self, $map, $source_col, $magic_map ) = @_;
    if ( $source_col->{att}->{TOINSTANCETYPE} eq $self->target ) {
        return $source_col->{att};
    }

    my ( $field, $inst, $type ) =
      map { $source_col->{att}->{$_} } qw /TOFIELD TOINSTANCE TOINSTANCETYPE/;

    if ( my $new_field = $magic_map->{$inst}->{$type}->{$field} ) {
        $field = $new_field;
    }

    foreach my $next_col (
        $map->findnodes(
            qq{.//CONNECTOR[\@FROMFIELD="$field" 
and \@FROMINSTANCE="$inst" 
and \@FROMINSTANCETYPE="$type"]}
        )
      )
    {
        my $result = $self->recursive_mapping( $map, $next_col, $magic_map );
        return $result if $result;
    }
}

sub source_struct {
    my ( $self, $inst, $map ) = @_;
    if ( !$self->cache->{source_kind}->{ $map->{att}->{NAME} }->{$inst} ) {
        my ($source) = $map->findnodes(qq{//SOURCE[\@NAME="$inst"]});
        $self->cache->{source_kind}->{ $map->{att}->{NAME} }->{$inst} =
          $source->{att};

    }
    return $self->cache->{source_kind}->{ $map->{att}->{NAME} }->{$inst};
}

sub target_struct {
    my ( $self, $inst, $map ) = @_;

    my $map_name = $map->{att}->{NAME};
    my ( $infs, $table_name );

    if ( !$self->cache->{$map_name}->{$inst} ) {
        $infs = $self->target_map($inst);
        $table_name = $self->map_instance( $map, $inst );
        $self->cache->{$map_name}->{$inst}->{infs}       = $infs;
        $self->cache->{$map_name}->{$inst}->{table_name} = $table_name;
    }
    else {
        $infs       = $self->cache->{$map_name}->{$inst}->{infs};
        $table_name = $self->cache->{$map_name}->{$inst}->{table_name};
    }

    return {
        repository => $self->get_repository,
        folder     => $self->get_folder,
        db_type    => $infs->{db},
        conn       => $infs->{conn},
        table_name => $table_name,
        map_name   => $map_name,
    };
}

sub map_instance {
    my ( $self, $map, $inst_name ) = @_;
    my ($table_name) = $map->findnodes(qq{//INSTANCE[\@NAME="$inst_name"]});
    return $table_name->{att}->{TRANSFORMATION_NAME};
}

sub target_map {
    my ( $self, $map_name ) = @_;
    my $target_type = $self->target;
    my ($meta_infs) =
      $self->parser->findnodes(
qq{//SESSION/SESSIONEXTENSION[\@TRANSFORMATIONTYPE="$target_type" and \@SINSTANCENAME="$map_name"]/CONNECTIONREFERENCE}
      );
    return {
        db   => $meta_infs->{att}->{CONNECTIONSUBTYPE},
        conn => $meta_infs->{att}->{CONNECTIONNAME}
    };
}

sub transformation_magic {
    my ( $self, $map ) = @_;
    my $magic_map       = {};
    my @transformations = $map->findnodes('.//TRANSFORMATION');
    foreach my $trans (@transformations) {
        my @inputs = $trans->findnodes('.//TRANSFORMFIELD[@PORTTYPE="INPUT"]');
        my @outputs =
          $trans->findnodes('.//TRANSFORMFIELD[@PORTTYPE="OUTPUT"]');
        foreach my $input (@inputs) {
            my $name_in = $input->{att}->{NAME};
            my ($name_out) =
              map { $_->{att}->{NAME} }
              grep {
                     $_->{att}->{EXPRESSION}
                  && $_->{att}->{EXPRESSION} =~ /$name_in/;
              } @outputs;

            if ($name_out) {
                $magic_map->{ $trans->{att}->{NAME} }
                  ->{ $trans->{att}->{TYPE} }->{$name_in} = $name_out;
            }
        }
    }
    return $magic_map;
}

42;

__END__
=head1 NAME

Parser::PowerCenter::XML - The great new Parser::PowerCenter::XML!

=head1 VERSION

Version 0.01

=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use Parser::PowerCenter::XML;

    my $foo = Parser::PowerCenter::XML->new();
    ...

=head1 SUBROUTINES/METHODS

=head2 function2

=cut

=head1 AUTHOR

Daniel de Oliveira Mantovani, C<< <daniel.oliveira.mantovani at gmail.com> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-parser-powercenter-xml at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Parser-PowerCenter-XML>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.




=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Parser::PowerCenter::XML


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker (report bugs here)

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=Parser-PowerCenter-XML>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Parser-PowerCenter-XML>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Parser-PowerCenter-XML>

=item * Search CPAN

L<http://search.cpan.org/dist/Parser-PowerCenter-XML/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2012 Daniel de Oliveira Mantovani.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.
