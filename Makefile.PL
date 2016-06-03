use ExtUtils::MakeMaker;
use strict;
use warnings;


# check configuration
my $home = $ENV{"HOME"};
$ENV{"PATH"} = $ENV{"PATH"} . ":/usr/local/bin:$home/bin:.:..:../..";
my $inc = '';
chomp($inc);
if (length($inc) < 2) {
    $inc = "-I/usr/local/include -I$home/include -I../librdkafka-0.9.0-2/src -I/usr/include -I.";
}
my $libs = '';
chomp($libs);
if(length($libs) < 2){
    $libs = "-L/usr/local/lib -L$home/lib -L/usr/lib/x86_64-linux-gnu -L." .
        " -lrdkafka -lz -lrt -lpthread";
}


# Create Makefile
WriteMakefile(
    'NAME'              => 'KafkaClient',
    'VERSION_FROM'      => 'KafkaClient.pm',
    'INC'               => $inc,
    'LIBS'              => $libs,
    'clean'             => { FILES => "*~ *.tmp *.tar *.tar.gz hoge moge" },
    );
