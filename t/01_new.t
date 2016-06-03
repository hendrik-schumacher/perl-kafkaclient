use strict;
use Test::More tests => 2;

use KafkaClient;

ok(defined KafkaClient::Producer->new(), "instantiate producer");
ok(defined KafkaClient::Consumer->new(), "instantiate consumer");

