# -*- perl -*-

require 5.006;
use strict;
use vars qw($DBI_INC_DIR);
require ExtUtils::MakeMaker;

eval { require 5.006 } or die << 'EOD';
#######################################
#	This module requires a minimum Perl version of 5.006
#	Please upgrade!
#######################################
EOD

print "\nConfiguring DBD::Teradata ...\n
>>>\tRemember to *READ* the README file!
   \tAnd re-read it if you have any problems.\n
";

my %prereq;
my %opts =
    ('NAME' => 'DBD::Teradata',
     'VERSION_FROM' => 'Teradata.pm',
     'dist'         => { 
	 	'DIST_DEFAULT' => 'all tardist',
     	'SUFFIX'       => 'gz',
	 	'COMPRESS'     => 'gzip -9vf' }
     );

if ($ExtUtils::MakeMaker::VERSION >= 5.43) {
    $opts{'AUTHOR'} = 'Dean Arnold (darnold@presicient.com)';
    $opts{'ABSTRACT'} = 'Teradata driver for DBI module';
    $opts{'PREREQ_PM'} = \%prereq;
}
my $ok = 1;
$ok &&= CheckModule('DBI', '1.13');

print "
	Missing modules are available from any CPAN mirror, for example,
	http://search.cpan.org

"
	unless $ok;

print "
Can't find current Crypt::Blowfish or Crypt::Blowfish_PP 
	(required for V2R5.1 encryption support).
	Missing modules are available from any CPAN mirror, for example,
	http://search.cpan.org

"
	unless (CheckModule('Crypt::Blowfish', '2.09') ||
		CheckModule('Crypt::Blowfish_PP', '1.12'));

print "
Can't find current Math::BigInt 
	(required for V2R5.1 encryption support).
	Missing modules are available from any CPAN mirror, for example,
	http://search.cpan.org

"
	unless CheckModule('Math::BigInt', '1.60');


&check_DBI();
ExtUtils::MakeMaker::WriteMakefile(%opts);

sub check_DBI {
    my $dir;
    eval { require DBI::DBD };
    foreach $dir (@INC) {
        if (-f "$dir/auto/DBI/DBIXS.h") {
  	    $DBI_INC_DIR = "-I$dir/auto/DBI";
	    last;
        }
     }
    if (!defined($DBI_INC_DIR)) {
        print "Unable to locate auto/DBI/DBIXS.h in the following directories:\n";
        print "\n";
        foreach $dir (@INC) {
    	    print "    $dir\n";
        }
        print "\nDoesn't look like a correct DBI installation.\n";
        $ok = 0;
    }
    if (!$ok) {
        exit(0);
    }
}

sub CheckModule ($$;$) {
    my($module, $version, $package) = @_;
    $package ||= $module;
    $prereq{$module} = $version || 0;
    if ($version) {
		print "Checking for $package, $version or later ... ";
		eval "use $module $version";
		if ($@) {
		    print("\nYou don't have the $package package",
			  " version $version or later, installed.\n");
		    return 0;
		}
    } else {
		print "Checking for $package ... ";
		eval "use $module";
		if ($@) {
		    print("\nYou don't have the $package package installed.\n");
		    return 0;
		}
    }
    print "ok\n";
    return 1;
}

package MY;

sub postamble {
    "\npm_to_blib: README\n" .
    "\nREADME: Teradata.pm" .
    "\n\tperldoc -t Teradata.pm >README\n\n"
}


sub libscan {
    my($self, $path) = @_;
    ($path =~ /\~$/) ? undef : $path;
}
