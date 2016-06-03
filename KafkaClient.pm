package KafkaClient;

use strict;
use warnings;
use 5.008_001;

use Exporter;
use XSLoader;
use base qw(Exporter);

BEGIN {
	our $VERSION = '1.01';
	XSLoader::load('KafkaClient', $VERSION);
}

sub DESTROY {
	my ($self) = @_;
	kc_destroy($self->[0]);
}

sub dump {
	my ($self) = @_;
	kc_dump($self->[0]);
}

sub add_broker {
	my ($self, $brokerlist) = @_;
	return kc_add_broker($self->[0], $brokerlist);
}

sub poll {
	my ($self) = @_;
	kc_poll($self->[0]);
}

sub new_topic {
	my ($self, $topic) = @_;
	return KafkaClient::Topic->new($self->[0], $topic);
}

sub get_topics {
	my ($self) = @_;
	return kc_get_topics($self->[0]);
}

sub get_brokers {
        my ($self) = @_;
        return kc_get_brokers($self->[0]);
}

sub set_dr_callback {
	my ($self, $callback) = @_;
	kc_set_dr_cb($self->[0], $callback);
}

sub set_stats_callback {
	my ($self, $callback) = @_;
	kc_set_stats_cb($self->[0], $callback);
}

sub set_log_callback {
	my ($self, $callback) = @_;
	kc_set_log_cb($self->[0], $callback);
}

sub set_error_callback {
	my ($self, $callback) = @_;
	kc_set_error_cb($self->[0], $callback);
}

package KafkaClient::Producer;
use base "KafkaClient";

sub new {
	my ($class, %params) = @_;
	my $self = [0, undef, undef];
	my $config = 0;
	if (scalar(keys %params) > 0) {
		$config = KafkaClient::kc_config_new();
		foreach my $name (keys %params) {
			KafkaClient::kc_config_set($config, $name, $params{$name});
		}
	}
	$self->[0] = KafkaClient::kc_new(1, $config);
	bless $self, $class;
	return $self;
}

package KafkaClient::Consumer;
use base "KafkaClient";

sub new {
	my ($class, %params) = @_;
	my $self = [0, undef, undef];
	my $config = 0;
	if (scalar(keys %params) > 0) {
		$config = KafkaClient::kc_config_new();
		foreach my $name (keys %params) {
			KafkaClient::kc_config_set($config, $name, $params{$name});
		}
	}
	$self->[0] = KafkaClient::kc_new(2, $config);
	bless $self, $class;
	return $self;
}

package KafkaClient::Topic;

sub new {
	my ($class, $producer, $topic, %params) = @_;
	my $self = [0, $producer, undef];
	my $config = 0;
	if (scalar(keys %params) > 0) {
		$config = kc_topic_config_new();
		foreach my $name (keys %params) {
			kc_topic_config_set($config, $name, $params{$name});
		}
	}
	$self->[0] = kc_topic_new($producer, $topic, 0);
	bless $self, $class;
	return $self;
}

sub DESTROY {
	my ($self) = @_;
	kc_topic_destroy($self->[0]);
}

sub produce {
	my ($self, $buf, $partition, $msg_id, $key) = @_;
	if (defined $key) {
		return kc_topic_produce_with_key($self->[0], $buf, $partition, $msg_id, $key);
	} else {
		return kc_topic_produce($self->[0], $buf, $partition, $msg_id);
	}
}

# $startoffset can be 0..N for given offset, -2 for beginning of kafka partition queue, -1 for end of kafka partition queue, -1000 for offset retrieved from offset store
sub consume {
	my ($self, $partition, $startoffset, $timeout, $callback) = @_;
	my $error = kc_topic_consume_start($self->[0], $partition, $startoffset);
	if ($error == 0) {
		kc_topic_consume($self->[0], $partition, $callback, $timeout);
		$error = kc_topic_consume_stop($self->[0], $partition);
	}
	return $error;
}

sub consume_queue {
	my ($self, $partitions, $startoffset, $timeout, $callback) = @_;
	my $rkqu = kc_create_queue($self->[1]);
	my $error = 0;
	foreach my $partition (@$partitions) {
		$error = kc_topic_consume_start_queue($self->[0], $partition, $startoffset, $rkqu);
		last if $error != 0;
	}
	if ($error == 0) {
		kc_topic_consume_queue($rkqu, $callback, $timeout);
		foreach my $partition (@$partitions) {
			$error = kc_topic_consume_stop($self->[0], $partition);
		}
	}
	kc_destroy_queue($rkqu);
	return $error;
}

sub get_partitions {
	my ($self) = @_;
	return kc_get_partitions($self->[1], $self->[0]);
}

1;

__END__

=head1 KafkaClient

Perl Client for Apache Kafka using librdkafka XS bindings.

Offers producer and consumer functionality without zookeeper support.

Compatible with librdkafka 0.9.0.

=head2 Usage:

=head3 KafkaClient::Producer

  use KafkaClient;

  my $delivered = 0;
  sub dr_callback {
    my ($result, $message, $msg_id) = @_;
    $delivered++;
    if ($result != 0) {
      # error occured
    } else {
      # success, message with id $msg_id was sent
    }
  }

  my $kcp = KafkaClient::Producer->new("compression.codec" => "gzip", "message.max.bytes" => 10000000);
  $kcp->add_broker($broker_address);
  $kcp->set_dr_callback(\&dr_callback);

  my $sent = 0;
  my $id = 0;
  my $kct = $kcp->new_topic($topic);
  # produce one message
  my $rcode = $kct->produce($message, undef, $id++);
  if ($rcode != 0) {
    # error when sending, there will be no delivery for this message
  }
  else {
    $sent++;
  }
  # poll until everything is delivered
  while ($delivered < $sent) {
    $kcp->poll();
  }

=head3 KafkaClient::Consumer

  use KafkaClient;

  my $kcc = KafkaClient::Consumer->new("group.id" => "test");
  $kcc->add_broker($broker_address);

  sub consume_callback {
    my ($rcode, $topic, $partition, $key, $message, $offset) = @_;
    if ($rcode == 0) {
      # got message
    } else {
      # got error
    }
  }

  my $kct = $kcc->new_topic($topic);
  my ($partition_cnt, @partitions) = $kct->get_partitions();
  # loop through all partitions for this topic and consume
  foreach my $partition (@partitions) {
    # offset=-2 means start from beginning
    $kct->consume($partition, -2, 2000, \&consume_callback);
  }
  $kcc->poll();

=head1 Setup

To use this module you need librdkafka source in

../librdkafka-0.9.0-1/src

(otherwise you have to change Makefile.PL). You need to build this library (./configure and make).

Then run

  perl Makefile.PL
  make

(and optionally make install).

To build the debian package (currently supporting jessie) run (from parent dir):

  export DEB_BUILD_OPTIONS=nocheck
  dh-make-perl make perl-kafkaclient --recursive --core-ok --build --notest --vcs ''

=head1 AUTHOR

Hendrik Schumacher <hendrik.schumacher@meetrics.de>

=head1 COPYRIGHT

Copyright (c) 2012 Hendrik Schumacher <hendrik.schumacher@meetrics.de>. All rights reserved.
This program is free software; you can redistribute it and/or
modify it under the same terms as Perl itself.

=cut

# END OF FILE
