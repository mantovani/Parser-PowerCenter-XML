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
        my $map_name = $map->{att}->{NAME};

        #next unless $map_name =~ /M_OS3_MOVE_TABELAS_DIMENSAO/i;
        my $magic_map = $self->transformation_magic($map);
        push @struct, [ $map_name, @{ $self->mapping( $map, $magic_map ) } ];
    }
    return \@struct;
}

sub get_infs_target {
    my $self = shift;
    my @maps = $self->get_maps;
    my @struct;
    foreach my $map (@maps) {
        my $map_name = $map->{att}->{NAME};
        push @struct, [ $map_name, @{ $self->mapping_target($map) } ];
    }
    return \@struct;
}

sub get_infs_source {
    my $self = shift;
    my @maps = $self->get_maps;
    my @struct;
    foreach my $map (@maps) {
        my $map_name = $map->{att}->{NAME};
        push @struct, [ $map_name, @{ $self->mapping_source($map) } ];
    }
    return \@struct;
}

sub get_workflow {
    my ( $self, $map_name ) = @_;
    my ($session) =
      $self->parser->findnodes(qq{//SESSION[\@MAPPINGNAME="$map_name"]});
    my $session_name = $session->{att}->{NAME};
    my ($task_inst) =
      $self->parser->findnodes(
        qq{//WORKFLOW/TASKINSTANCE[\@NAME="$session_name"]});
    if ($task_inst) {
        my ($workflow) = $task_inst->parent;
        return $workflow->{att}->{NAME};
    }
    else { return 'NULL' }
}

sub mapping_target {
    my ( $self, $map ) = @_;
    my $workflow = $self->get_workflow( $map->{att}->{NAME} );
    my @struct;
    my $target_type = $self->target;
    my @target_cols =
      $map->findnodes(qq{.//CONNECTOR[\@TOINSTANCETYPE="$target_type"]});
    foreach my $target (@target_cols) {
        push @struct,
          {
            target_physical_column => $target->{att}->{TOFIELD},
            %{ $self->target_struct( $target->{att}->{TOINSTANCE}, $map ) },
            workflow => $workflow,
          };
    }
    return \@struct;
}

sub mapping_source {
    my ( $self, $map ) = @_;
    my $workflow = $self->get_workflow( $map->{att}->{NAME} );
    my @struct;
    my $source_type = $self->source;
    my @source_cols =
      $map->findnodes(qq{.//CONNECTOR[\@FROMINSTANCETYPE="$source_type"]});
    foreach my $source_col (@source_cols) {
        my $source = $source_col->{att};
        push @struct,
          {
            source_field => $source->{FROMFIELD},
            %{
                $self->source_struct( $source->{FROMINSTANCE},
                    $map, $source->{FROMFIELD} )
            },
            workflow => $workflow,
          };
    }
    return \@struct;
}

sub mapping {
    my ( $self, $map, $magic_map ) = @_;
    my $workflow = $self->get_workflow( $map->{att}->{NAME} );
    my @struct;
    my $source_type = $self->source;
    my @source_cols =
      $map->findnodes(qq{.//CONNECTOR[\@FROMINSTANCETYPE="$source_type"]});
    foreach my $source_col (@source_cols) {
        my $source = $source_col->{att};
        my $targets =
          $self->recursive_mapping( $map, $source_col, $magic_map, [],
            $self->source );
        foreach my $target ( @{$targets} ) {
            push @struct,
              {
                source_field => $source->{FROMFIELD},
                %{
                    $self->source_struct( $source->{FROMINSTANCE},
                        $map, $source->{FROMFIELD} )
                },
                target_physical_column => $target->{TOFIELD},
                %{ $self->target_struct( $target->{TOINSTANCE}, $map ) },
                workflow            => $workflow,
                transformation_flow => $target->{trans},
              };
        }
    }
    return \@struct;
}

sub recursive_mapping {
    my ( $self, $map, $source_col, $magic_map, $array_ref, $trans ) = @_;
    $trans .= ',' . $source_col->{att}->{TOINSTANCETYPE};
    if ( $source_col->{att}->{TOINSTANCETYPE} eq $self->target ) {
        $source_col->{att}->{trans} = $trans;
        return $source_col->{att};
    }

    my ( $field, $inst, $type ) =
      map { $source_col->{att}->{$_} } qw/TOFIELD TOINSTANCE TOINSTANCETYPE/;

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
        my $result =
          $self->recursive_mapping( $map, $next_col, $magic_map, [], $trans );

        if ( ref $result eq 'ARRAY' ) {
            $result = $result->[0];
        }
        if ( ref $result eq 'HASH' ) {
            push @{$array_ref}, $result;
        }
    }
    return $array_ref;
}

sub source_struct {
    my ( $self, $inst, $map, $field_name ) = @_;

    my $source_type = $self->source;
    my $real_inst   = $self->map_instance( $map, $inst, $self->source );
    my ($source)    = $map->findnodes(qq{//SOURCE[\@NAME="$real_inst"]});

    my ($column) = $source->findnodes(qq{.//SOURCEFIELD[\@NAME="$field_name"]});
    if ( !$column ) {
        ($column) = $source->findnodes(qq{//SOURCEFIELD[\@NAME="$field_name"]});
    }

    my $table_name;
    if ( $source->{att}->{DATABASETYPE} eq 'Flat File' ) {
        $table_name = $self->get_flat_file($inst);
    }
    else {
        $table_name = $self->map_instance( $map, $inst, $self->source );
    }

    my $source_db = $self->get_sourcedb( $inst, $map )
      || $source->{att}->{DBDNAME};
    my %struct = (
        source_database       => $source_db,
        source_owner          => $source->{att}->{OWNERNAME},
        source_type           => $source->{att}->{DATABASETYPE},
        source_datatype       => $column->{att}->{DATATYPE},
        source_physical_table => $table_name,
        repository            => $self->get_repository,
        folder                => $self->get_folder,
        load_program          => $map->{att}->{NAME},
    );
    return \%struct;
}

sub get_flat_file {
    my ( $self, $inst ) = @_;
    my ($conn_flat) = $self->parser->findnodes(
qq{//SESSIONEXTENSION[\@SINSTANCENAME="$inst"]//ATTRIBUTE[\@NAME="Source filename"]}
    );
    return $conn_flat->{att}->{VALUE} if $conn_flat;
}

sub get_sourcedb {
    my ( $self, $inst, $map ) = @_;
    my $inst_name;
    foreach my $sibling ( $map->next_siblings ) {
        ($inst_name) = $sibling->findnodes(
qq{.//SESSIONEXTENSION[\@SINSTANCENAME="$inst" and \@NAME="Relational Reader"]}
        );
        last if $inst_name;
    }

    $inst_name = $inst_name->{att}->{DSQINSTNAME};
    return unless $inst_name;

    foreach my $sibling ( $map->next_siblings ) {
        my ($conns) = $sibling->findnodes(
qq{.//SESSIONEXTENSION[\@SINSTANCENAME="$inst_name" and \@TRANSFORMATIONTYPE="Source Qualifier"]/CONNECTIONREFERENCE}
        );
        if ($conns) {
            return $conns->{att}->{CONNECTIONNAME};
        }
    }
}

sub target_struct {
    my ( $self, $inst, $map ) = @_;
    my $map_name = $map->{att}->{NAME};
    if ( !$self->cache->{target_struct}->{$map_name}->{$inst} ) {
        $self->cache->{target_struct}->{$map_name}->{$inst}->{infs} =
          $self->target_map( $inst, $map );
        $self->cache->{target_struct}->{$map_name}->{$inst}->{table_name} =
          $self->map_instance( $map, $inst, $self->target );
    }
    my ( $table_name, $infs ) = (
        $self->cache->{target_struct}->{$map_name}->{$inst}->{table_name},
        $self->cache->{target_struct}->{$map_name}->{$inst}->{infs}
    );
    return {
        repository            => $self->get_repository,
        folder                => $self->get_folder,
        db_type               => $infs->{database},
        conn                  => $infs->{connection},
        frequency_movement    => $infs->{frequency_movement},
        target_physical_table => $table_name,
        load_program          => $map_name,
    };
}

sub map_instance {
    my ( $self, $map, $inst_name, $type ) = @_;
    if ( !$self->cache->{map_instance}->{ $map->{att}->{NAME} }->{$inst_name}
        ->{$type} )
    {
        my ($table_name) = $map->findnodes(
qq{//INSTANCE[\@NAME="$inst_name" and \@TRANSFORMATION_TYPE="$type"]}
        );
        $self->cache->{map_instance}->{ $map->{att}->{NAME} }->{$inst_name}
          ->{$type} = $table_name->{att}->{TRANSFORMATION_NAME};
    }
    return $self->cache->{map_instance}->{ $map->{att}->{NAME} }->{$inst_name}
      ->{$type};
}

sub target_map {
    my ( $self, $map_name, $map ) = @_;
    my $target_type = $self->target;
    foreach my $sibling ( $map->next_siblings ) {
        my ($meta_infs) =
          $sibling->findnodes(
qq{.//SESSIONEXTENSION[\@TRANSFORMATIONTYPE="$target_type" and \@SINSTANCENAME="$map_name"]/CONNECTIONREFERENCE}
          );
        if ($meta_infs) {
            my ($frequency_movement) =
              grep { $_->{att}->{VALUE} eq 'YES' } $meta_infs->next_siblings;
            return {
                database           => $meta_infs->{att}->{CONNECTIONSUBTYPE},
                connection         => $meta_infs->{att}->{CONNECTIONNAME},
                frequency_movement => $frequency_movement->{att}->{NAME},
            };
        }
    }
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
