#!/usr/bin/perl
#
#	fastload.pl - script to emulate FASTLOAD command utility
#
#	Coded 11/28/2000	by Dean Arnold
#
#	Copyright(C) 2000 Dean Arnold
#
require 5.005;
use DBI;
use DBI qw{:sql_types};
use IO::Seekable qw{SEEK_CUR};

my $VERSION = 1.00;

my %typestr = (
	SQL_VARCHAR, 'VARCHAR', 
	SQL_CHAR, 'CHAR', 
	SQL_FLOAT, 'FLOAT', 
	SQL_DECIMAL, 'DECIMAL',
	SQL_INTEGER, 'INTEGER', 
	SQL_SMALLINT, 'SMALLINT', 
	SQL_TINYINT, 'BYTEINT', 
	SQL_VARBINARY, 'VARBYTE',
	SQL_BINARY, 'BYTE',
	SQL_LONGVARBINARY, 'VARBYTE',
	SQL_DATE, 'DATE',
	SQL_TIMESTAMP, 'TIMESTAMP',
	SQL_TIME, 'TIME'
	);

%cmds = (
	'AXSMOD', \&nosupp,
	'CLEAR', \&clear,
	'CT', \&execsql,
	'DATABASE', \&execsql,
	'DATEFORM', \&dateform,
	'DEFINE', \&def_rec,
	'DEF', \&def_rec,
	'DELETE', \&execsql,
	'DEL', \&execsql,
	'ERRLIMIT', \&errlimit,
	'HELP', \&help,
	'INSERT', \&ins_cmd,
	'INS', \&ins_cmd,
	'LOGOFF', \&logoff,
	'LOGON', \&logon,
	'NOTIFY', \&nosupp,
	'OS', \&os_cmd,
	'QUIT', \&quit,
	'RECORD', \&record,
	'SESSIONS', \&set_sess,
	'SHOW', \&show_cmd,
	'TENACITY', \&nosupp
);

%cmds2 = (
	'BEGIN LOADING', \&begin_load,
	'CREATE TABLE', \&execsql,
	'DROP TABLE', \&execsql,
	'END LOADING', \&end_load,
	'HELP TABLE', \&help_tbl,
	'SET RECORD', \&set_rec,
	'SET SESSION', \&nosupp,
	'SHOW VERSIONS', \&versions
);

%logon1st = (
	'BEGIN LOADING', 1,
	'CREATE TABLE', 1,
	'DROP TABLE', 1,
	'END LOADING', 1,
	'HELP TABLE', 1,
	'CT', 1,
	'DATABASE', 1,
	'DELETE', 1,
	'DEL', 1,
	'INSERT', 1,
	'INS', 1
);

my @declens = ( 0, 1, 1, 2, 2, 4, 4, 4, 4, 4, 8, 8, 8, 8, 8, 8, 8, 8, 8);
my %typelen = ( 'INT', 4, 'INTEGER', 4, 'BYTEINT', 1, 'SMALLINT', 2, 'FLOAT', 8, 'DATE', 4, 
	'LONG VARCHAR', 32000, 'LONG VARBYTE', 32000);
my %typeabr = ( 'INT', 'INTEGER', 'DEC', 'DECIMAL', 'CHARACTER', 'CHAR', 'CHARACTERS', 'CHAR' );

my $numsess = 0;
my $drh;			# driver handle
my @fastdbh = ();	# array of fastload sessions
my $ctldbh;			# control session
my $errdbh;			# error session
my $loading = 0;	# 1 => BEGIN LOADING issued
my $ins_stmt = '';	# INSERT statement for load
my $using = '';		# USING clause generated from DEFINE
my $ckpt = -1;		# checkpoint count; -1 means none
my $ttlrows = 0;	# total rows transfered
my $errlimit = 0;	# max errors allowed before terminate
my $i;
my $remnant = '';
my $first = '';
my $second = '';
my @fielddefs = ();	# DEFINE'd input fields
my $indics = 0;		# 1 => use indicators
my $dateformat = 0; # 1 => use ANSI, else integer
my $startrec = 0;	# first record to load, starting from 0
my $endrec = undef;	# last record to load, undef means all
my $recfmt = 'f';	# f => formatted, u => unformatted, v => vartext
my $vtsep = '|';	# vartext separator
my $vtdisp = 0;		# 1 => display invalid vartext input
my $vtnostop = 0;	# 1 => don't stop on invalid vartext input
my $maxses = undef;	# undef => logon to all AMPs
my $minses = 1;		# min number of session to logon
my $infilenm = '';	# input data file name
my @fldname = ();	# input data field definitions
my %fldtype = ();
my %fldprec = ();
my %fldnullif = ();
my $inf;				# input data file handle
my $table = '';
my $insstmt = '';
my $err1 = '';
my $err2 = '';
my $file_in = 0;	# 1 => command file input

print "Enter FASTLOAD command:\n";
my $incmd = 0;
my $dotcmd = 0;
my $cmd = '';
my $cmdf = *STDIN;
if ($ARGV[0]) {
	open($cmdf, $ARGV[0]) || die "Can't open command file: $!\n";
	$file_in = 1;
}
while (<$cmdf>) {
#
#	accumulate complete command string
#
	chop;
	if ($incmd == 0) {
		if ($_=~/^\s*\./) { 
			$dotcmd = 1;
			$_=~s/^\s*\.//;
		}
		else { $dotcmd = 0; }
		$cmd = $_;
	}
	else {
		$cmd .= ' ' . $_;
	}
	if (($dotcmd == 0) && ($cmd!~/;\s*$/)) { 
		$incmd = 1;
		next; 
	}
#
#	got a complete command, process it
#
	$dotcmd = 0;
	$incmd = 0;
	$cmd =~s/;\s*$//;
	
	if ($cmd=~/^\s*(\S+)\s+(\S+)(.*)$/) {
		$first = uc $1; 
		$second = uc $2;
		$remnant = $3;
	}
	elsif ($cmd=~/^\s*(\S+)(.*)$/) {
		$first = uc $1; 
		$remnant = $2;
		$second = '';
	}
	else { next; }
	
	$remnant=~s/^\s+//;
	$remnant=~s/\s+$//;
	
	if ($file_in) { print "$cmd\n"; }
	my $handler;
	if (defined($cmds2{($first . ' ' . $second)})) {
		if ((!defined($ctldbh)) && defined($logon1st{($first . ' ' . $second)})) {
			print STDERR "You must be logged on to do that.\n";
			print "Enter FASTLOAD command:\n";
			next;
		}
		$handler = $cmds2{($first . ' ' . $second)};
		&$handler(($first . ' ' . $second), $remnant);
	}
	elsif (defined($cmds{$first})) {
		if ((!defined($ctldbh)) && defined($logon1st{$first})) {
			print STDERR "You must be logged on to do that.\n";
			print "Enter FASTLOAD command:\n";
			next;
		}
		if ($second ne '') {
			$remnant = $second . ' ' . $remnant;
			$remnant=~s/\s+$//;
		}
		$handler = $cmds{$first};
		&$handler($first, $remnant);
	}
	else {
		print STDERR "Unrecognized command \"$first $second\".\n";
	}
	print "Enter FASTLOAD command:\n";
}

print "End of input, exitting...";
wrapup();


sub quit {
	my ($cmd, $remnant) = @_;
	if ($remnant ne '') {
		print STDERR "Extraneous command text ignored.\n";
	}
#
#	wait for pending requests to complete,
#	logoff everything
#	exit
#
	if (defined($ctldbh)) {
		logoff('', '');
	}
	exit 0;
}

sub logon {
	my ($cmd, $remnant) = @_;

	if (scalar(@fastdbh) != 0) {
		print STDERR "Already logged on.\n";
		return 1;
	}
	if ($remnant eq '') {
		print STDERR "No logonstring provided.\n";
		return 1;
	}
	if ($remnant=~/^([^\/]+)\/([^,]+),(\S+)$/) {
		$tdp = 'dbi:Teradata:' . $1;
		$user = $2;
		$pass = $3;
	}
	else {
		print STDERR "Invalid logonstring provided.\n";
		return 1;
	}
#
#	logon control session
#	logon error session
#	logon N fastload sessions
#
	if (!defined($maxses)) { $maxses = 24; }

	$ctldbh = DBI->connect($tdp, $user, $pass,
		{
			PrintError => 0,
			RaiseError => 0,
			AutoCommit => 1,
			tdat_lsn => 0
		}
	);
	if (!defined($ctldbh)) {
		print STDERR ($drh->errstr . "\n");
		return 1;
	}
	my $lsn = $ctldbh->{'tdat_lsn'};
	$drh = $ctldbh->{'Driver'};
	
	$errdbh = DBI->connect($tdp, $user, $pass,
		{
			PrintError => 0,
			RaiseError => 0,
			AutoCommit => 1,
			tdat_lsn => 0
		}
	);
	if (!defined($errdbh)) {
		print STDERR $drh->errstr . "\n";
		$ctldbh->disconnect;
		undef $ctldbh;
		return 1;
	}
	
	my $i;
	foreach $i (0..$maxses-1) {
		my $dbh = DBI->connect($tdp, $user, $pass,
			{
			PrintError => 0,
			RaiseError => 0,
			AutoCommit => 0,
			tdat_lsn => $lsn,
			tdat_utility => FASTLOAD
			});
		if (!defined($dbh)) {
			last;
		}
		$fastdbh[$i] = $dbh;
	}
	$i = scalar(@fastdbh);
	if (($i >= $minses) && 
		(($DBI::err == 0) || ($DBI::err == 2632))) {
		if ($DBI::err != 0) { print STDERR ($DBI::errstr . "\n"); }
		print STDERR "$i FASTLOAD sessions logged on.\n";
		return 1;
	}
	if ($DBI::err != 0) { 
		print STDERR ('Failure ' . $DBI::err . ': ' . $DBI::errstr . "\n"); 
	}
	elsif ($i < $minses) {
		print STDERR "Cannot logon minimum number of FASTLOAD sessions.\n";
		print STDERR "Requested minimum was $minses, but only got $i.\n";
	}
	foreach my $dbh (@fastdbh) {
		$dbh->disconnect;
	}
	@fastdbh = ();
	$errdbh->disconnect;
	$ctldbh->disconnect;
	undef $errdbh;
	undef $ctldbh;
	return 1;
}

sub nosupp {
	my ($cmd, $remnant) = @_;
	
	print STDERR "$cmd not supported in this release.\n";
	return 1;
}

sub logoff {
	my ($cmd, $remnant) = @_;
	if ($remnant ne '') {
		print STDERR "Extraneous command text ignored.\n";
	}
#
#	logoff fastload sessions
#	logoff error session
#	logoff control session
#
	if (!defined($ctldbh)) {
		return 1;
	}
	foreach my $dbh (@fastdbh) {
		$dbh->disconnect;
	}
	@fastdbh = ();
	
	$errdbh->disconnect;
	$ctldbh->disconnect;
	undef $errdbh;
	undef $ctldbh;
	print STDERR "Logged off.\n";
	return 1;
}

sub	clear {
	my ($cmd, $remnant) = @_;
	@fldname = ();
	%fldprec = ();
	%fldtype = ();
	%fldnullif = ();
	$using = '';
	print STDERR "Warning: All previous column and file definitions cleared.\n";
	return 1;
}

sub execsql {
	my ($cmd, $remnant) = @_;
	my $rc = $ctldbh->do("$cmd $remnant");
	if (!defined($rc)) {
		print STDERR $ctldbh->errstr . "\n";
	}
	return 1;
}

sub dateform {
	my ($cmd, $remnant) = @_;
	
	if ($remnant=~/^INTEGERDATE$/i) {
		$dateformat = 0;
	}
	elsif ($remnant=~/^ANSIDATE$/i) {
		$dateformat = 1;
	}
	else {
		print STDERR "Invalid DATEFORM specified. INTEGERDATE assumed.\n";
		$dateformat = 0;
	}
	return 1;
}

sub def_rec {
	my ($cmd, $remnant) = @_;
	my $cols = '';
	if ($remnant=~/^(.*)\s+(FILE|INMOD)\s*\=\s*(\S+)$/i) {
		$cols = ',' . $1; 
		my $type = $2; 
		$infilenm = $3;
		if ($type eq 'INMOD') {
			print STDERR "INMOD support unavailable this release.\n";
			$infilenm = '';
			return 1;
		}
	}
	else {
		$cols = ',' . $remnant; 
	}
	
	if ($cols eq ',') {
		print STDERR "Field definitions will be taken from target table definition.\n";
		return 1;
	}
#
#	now collect the fields and types
#
	my ($name, $prec, $nullif);
	while ($cols ne '') {
		if ($cols=~/^\s*,\s*(\w+)\s*\(\s*(BYTE|VARBYTE|CHAR|VARCHAR|CHARACTERS|CHARACTER|DECIMAL|DEC|GRAPHIC)\s*\(([^\)]+)\)\s*,\s*NULLIF\s*\=\s*([^\)]+)\)/i) {
			$name = $1; $type = uc $2; $prec = $3; $nullif = $4;
			$cols=~s/^\s*,\s*\w+\s*\(\s*(BYTE|VARBYTE|CHAR|VARCHAR|CHARACTERS|CHARACTER|DECIMAL|DEC|GRAPHIC)\s*\([^\)]+\)\s*,\s*NULLIF\s*\=\s*[^\)]+\)(.*)$/$2/i;
		}
		elsif ($cols=~/^\s*,\s*(\w+)\s*\(\s*(BYTEINT|SMALLINT|INT|INTEGER|DATE|FLOAT)\s*,\s*NULLIF\s*\=\s*([^\)]+)\)/i) {
			$name = $1; $type = uc $2; $prec = ''; $nullif = $3;
			$cols=~s/^\s*,\s*\w+\s*\(\s*(BYTEINT|SMALLINT|INT|INTEGER|DATE|FLOAT)\s*,\s*NULLIF\s*\=\s*[^\)]+\)(.*)$/$2/i;
		}
		elsif ($cols=~/^\s*,\s*(\w+)\s*\(\s*(BYTE|VARBYTE|CHAR|VARCHAR|CHARACTERS|CHARACTER|DECIMAL|DEC|GRAPHIC)\s*\(([^\)]+)\)\s*\)/i) {
			$name = $1; $type = uc $2; $prec = $3; $nullif = '';
			$cols=~s/^\s*,\s*\w+\s*\(\s*(BYTE|VARBYTE|CHAR|VARCHAR|CHARACTERS|CHARACTER|DECIMAL|DEC|GRAPHIC)\s*\([^\)]+\)\s*\)(.*)$/$2/i;
		}
		elsif ($cols=~/^\s*,\s*(\w+)\s*\(\s*(BYTEINT|SMALLINT|INT|INTEGER|DATE|FLOAT)\s*\)/i) {
			$name = $1; $type = uc $2; $prec = ''; $nullif = '';
			$cols=~s/^\s*,\s*\w+\s*\(\s*(BYTEINT|SMALLINT|INT|INTEGER|DATE|FLOAT)\s*\)(.*)$/$2/i;
		}
		elsif ($cols=~/^\s*,\s*(\w+)\s*\(\s*LONG\s+(VARCHAR|VARBYTE)\s*,\s*NULLIF\s*\=\s*([^\)]+)\)/i) {
			$name = $1; $type = 'LONG ' . (uc $2); $prec = ''; $nullif = $3;
			$cols=~s/^\s*,\s*\w+\s*\(\s*LONG\s+(VARCHAR|VARBYTE)\s*,\s*NULLIF\s*\=\s*[^\)]+\)(.*)$/$2/i;
		}
		elsif ($cols=~/^\s*,\s*(\w+)\s*\(\s*LONG\s+(VARCHAR|VARBYTE)\s*\)/i) {
			$name = $1; $type = 'LONG ' . (uc $2); $prec = ''; $nullif = '';
			$cols=~s/^\s*,\s*\w+\s*\(\s*LONG\s+(VARCHAR|VARBYTE)\s*\)(.*)$/$2/i;
		}
		else {
			print STDERR "Unrecognized field definition at \"$cols\".\n";
			return 1;
		}

		if (($prec=~/,/) && ($type!~/^DEC(IMAL)?$/)) {
			print STDERR "Invalid precision for field $name.\n";
			return 1;
		}
		if ($type=~/^DEC(IMAL)?$/) {
			if (($prec=~/^\s*(\d+)/) && ($1 > 18)) {
				print STDERR "Precision too large for field $name.\n";
				return 1;
			}
			elsif (($prec=~/^\s*(\d+)\s*,\s*(\d+)/) && ($1 < $2)) {
				print STDERR "Scale too large for field $name.\n";
				return 1;
			}
		}
		if (defined($fldtype{$name})) {
			print STDERR "Field $name previously defined.\n";
			return 1;
		}
		push(@fldname, $name);
		if (defined($typeabr{$type})) { $type = $typeabr{$type}; }
		$fldtype{$name} = $type;
		$fldprec{$name} = $prec;
		$fldnullif{$name} = $nullif;
	}
	return 1;
}

sub errlimit {
	my ($cmd, $remnant) = @_;
	if ($remnant=~/^\d+$/) {
		$errlimit = $remnant;
	}
	else {
		print STDERR "Invalid ERRLIMIT specified.\n";
	}
	return 1;
}

sub help {
	my ($cmd, $remnant) = @_;
	print STDERR "Refer to fastload documentation.\n";
	return 1;
}


sub os_cmd {
	my ($cmd, $remnant) = @_;
	system($remnant);
	return 1;
}

sub record {
	my ($cmd, $remnant) = @_;
	if ($remnant=~/^(\d+)\s+THRU\s+(\d+)$/i) {
		$startrec = $1; $endrec = $2;
	}
	elsif ($remnant=~/^(\d+)$/i) {
		$startrec = $1; $endrec = undef;
	}
	elsif ($remnant=~/^THRU\s+(\d+)$/i) {
		$startrec = 0; $endrec = $2;
	}
	else {
		print STDERR "Invalid RECORD command.\n";
		print STDERR "Syntax: RECORD n [THRU m]"
	}
	return 1;
}

sub set_sess {
	my ($cmd, $remnant) = @_;
	
	if ($remnant=~/^(\d+)\s+(\d+)$/) {
		$maxses = $1; $minses = $2;
	}
	elsif ($remnant=~/^(\d+)$/) {
		$maxses = $1; $minses = 1;
	}
	elsif ($remnant=~/^\*\s+(\d+)$/) {
		$maxses = 24; $minses = $1;
	}
	elsif ($remnant=~/^(\d+)\s+\*$/) {
		$maxses = $1; $minses = 1;
	}
	elsif ($remnant=~/^\*$/) {
		$maxses = 24; $minses = 1;
	}
	elsif ($remnant=~/^\*\s+\*$/) {
		$maxses = 24; $minses = 1;
	}
	else {
		print STDERR "Invalid SESSIONS command.\n";
		return 1;
	}
	if (($minses > 24) || ($maxses > 24)) {
		print STDERR "Max sessions currently limited to 24.\n";
		$maxses = 24; $minses = 1;
	}
	elsif ($minses == 0) {
		print STDERR "Invalid minimum SESSIONS specified; assuming 1.\n";
		$minses = 1;
	}
	elsif ($maxses == 0) {
		print STDERR "Invalid maximum SESSIONS specified; assuming 24.\n";
		$minses = 1; $maxses = 24;
	}
	elsif ($maxses < $minses) {
		print STDERR "Invalid SESSIONS specified; min is greater than max. Assuming max=24 and min=1.\n";
		$maxses = 24; $minses = 1;
	}
	return 1;
}

sub show_cmd {
	my ($cmd, $remnant) = @_;
	
	if ($remnant ne '') {
		print STDERR "Invalid SHOW command.\n";
		return 1;
	}
	if ($infilenm ne '') {
		print "FILE = $infilenm\n";
	}
	if (scalar(@fldname) == 0) {
		print "TOTAL RECORD LENGTH = 0\n";
		return 1;
	}
	
	my $curroff = 0;
	my $name = '';
	foreach $name (@fldname) {
#
#	define format for output
#

		if ($fldtype{$name}=~/^DEC(IMAL)?$/) {
			my $len = $declens[$fldprec{$name}];
			print "$name\tOFFSET= $curroff\tLEN = $len\t$fldtype{$name}\n";
			$curroff += $len;
		}
		elsif ($fldprec{$name} ne '') {
			if ($fldtype{$name}=~/^VAR/) {
				my $len = $fldprec{$name} + 2;
				print "$name\tOFFSET= $curroff\tLEN = $len\t$fldtype{$name}\n";
				$curroff += $len;
			}
			else {
				print "$name\tOFFSET= $curroff\tLEN = $fldprec{$name}\t$fldtype{$name}\n";
				$curroff += $fldprec{$name};
			}
		}
		else {
			print "$name\tOFFSET= $curroff\tLEN = $typelen{$fldtype{$name}}\t$fldtype{$name}\n";
			$curroff += $typelen{$fldtype{$name}};
		}
	}
	print "TOTAL RECORD LENGTH= $curroff\n";
}

sub begin_load {
	my ($cmd, $remnant) = @_;
	$beginload = 'BEGIN LOADING ' . $remnant;
	$remnant .= ' ';
	if ($remnant=~/^(\S+)\s+ERRORFILES\s+([^\s,]+)\s*,\s*(\S+)\s/i) {
		$table = $1; $err1 = $2; $err2 = $3;
	}
	else {
		print STDERR "Invalid BEGIN LOADING command.\n";
		print STDERR "Syntax: BEGIN LOADING table ERRORFILES errortable1,errortable2 [CHECKPOINT n] [INDICATORS] ;\n";
		return 1;
	}
	
	if ($loading != 0) {
		print STDERR "BEGIN LOADING already active.\n";
		return 1;
	}
#
#	build the using clause
#
	$using = 'USING (';
	foreach $name (@fldname) {
		$using .= $name . ' ' . $fldtype{$name};
		if ($fldprec{$name} ne '') {
			$using .= '('. $fldprec{$name} . ')';
		}
		$using .= ', ';
	}
	$using = substr($using, 0, -2) . ')';
	if (length($using) < 10) {
		print STDERR "No fields defined.\n";
		return 1;
	}
	
	if ($infilenm eq '') {
		print STDERR "No input data file specified.\n";
		return 1;
	}
	elsif (! open($inf, "<$infilenm")) {
		print STDERR "Unable to open $infilenm: $!\n";
		return 1;
	}
	if ($recfmt ne 'v') {
		binmode $inf;
	}
	if ($remnant=~/\sCHECKPOINT\s+(\d+)\s/i) {
		$ckpt = $1;
	}
	else { 
		$ckpt = -1; 
	}
	if ($remnant=~/\sINDICATORS\s/i) {
		$indics = 1;
		$beginload=~s/\sINDICATORS//i;
	}
	else { $indics = 0; }
	
	$rc = $ctldbh->do($beginload);
	if (!defined($rc)) {
		print STDERR $ctldbh->errstr . "\n";
	}
	else { $loading = 1; }
	return 1;
}

sub end_load {
	my ($cmd, $remnant) = @_;

	$rc = $ctldbh->do('CHECKPOINT LOADING END;');
	if (!defined($rc)) {
		print STDERR $ctldbh->errstr . "\n" ;
	}
	$rc = $ctldbh->commit();
	if (!defined($rc)) {
		print STDERR $ctldbh->errstr . "\n";
	}
	$rc = $ctldbh->do('END LOADING;');
	if (!defined($rc)) {
		print STDERR $ctldbh->errstr . "\n";
	}
	$rc = $ctldbh->commit();
	if (!defined($rc)) {
		print STDERR $ctldbh->errstr . "\n";
	}
	print "Apply phase complete.\n";
	$loading = 0;
	return 1;
}

sub help_tbl {
	my ($cmd, $remnant) = @_;
	my $sth = $ctldbh->prepare("SHOW TABLE $remnant");
	if (!defined($sth)) {
		print STDERR $ctldbh->errstr . "\n";
		return 1;
	}
	if (!defined($sth->execute)) {
		print STDERR $sth->errstr;
		return 1;
	}
	while (my @row = $sth->fetchrow_array) {
		print "$row[0]\n";
	}
	$sth->finish;
	return 1;
}

sub set_rec {
	my ($cmd, $remnant) = @_;
	if ($remnant=~/^FORMATTED$/i) {
		print STDERR "Now set to read formatted records.\n";
		$recfmt = 'f';
	}
	elsif ($remnant=~/^FORMATTED$/i) {
		print STDERR "Now set to read unformatted records.\n";
		$recfmt = 'u';
	}
	elsif ($remnant=~/^VARTEXT(.*)$/i) {
		$remnant = $1;
		$recfmt = 'u';
		$vtsep = '|';
		$vtdisp = 0;
		$vtnostop = 0;
		if ($remnant=~/\"(.)\"/) {
			$vtsep = $1;
			$remnant=~s/\"(.)\"//;
		}
		if ($remnant=~/NOSTOP/i) {
			$vtnostop = 1;
			$remnant=~s/NOSTOP//i;
		}
		if ($remnant=~/DISPLAY_ERRORS/i) {
			$vtdisp = 1;
			$remnant=~s/DISPLAY_ERRORS//i;
		}
		$remnant=~s/\s+//g;
		if (length($remnant) != 0) {
			print STDERR "Unrecognized SET RECORD VARTEXT command; assuming defaults\n";
			$recfmt = 'u';
			$vtsep = '|';
			$vtdisp = 0;
			$vtnostop = 0;
		}
		print STDERR "Now set to read variable length text records.\n";
	}
	elsif ($remnant=~/^VARTEXT\s+$/i) {
		$recfmt = 'u';
		$vtsep = $1;
		$vtdisp = 0;
		$vtnostop = 0;
		print STDERR "Now set to read variable length text records.\n";
	}
	else {
		print STDERR "Unrecognized SET RECORD command.\n";
	}
	return 1;
}

sub versions {
	my ($cmd, $remnant) = @_;
	print STDERR "fastload.pl ver. $VERSION\n";
	return 1;
}

sub ins_cmd {
	my ($cmd, $remnant) = @_;
	if (!$loading) { return execsql($cmd, $remnant); }
#
#	build the using clause
#
	$ins_stmt = $using . ' ' . $cmd . ' ' . $remnant;
#
#	now start fastloading
#
	my $started = time;
	
	my $sth = $ctldbh->prepare($ins_stmt);
	if (!defined($sth)) {
		print STDERR $ctldbh->errstr . "\n";
	}
	$sth->finish;
	
	$ctldbh->{AutoCommit} = 0;
	$rc = $ctldbh->do($ins_stmt);
	if (!defined($rc)) {
		print STDERR $ctldbh->errstr . "\n";
		wrapup();
		return 1;
	}

	my @sth = ();
	my $i = 0;
	for ($i = 0; $i < scalar(@fastdbh); $i++) {
		$sth[$i] = $fastdbh[$i]->prepare(';',
			{
				tdat_raw => (($indics == 1) ? IndicatorMode : RecordMode),
				tdat_nowait => 1
			}
		);
		if (!defined($sth[$i])) {
			print STDERR $fastdbh[$i]->errstr;
			wrapup();
			return 1;
		}
	}
	$i--;
	my $total = 0;	
	my $threshold = 10000;
	my @outlist = (0..$i);
	my @parm_ary;
	my @actives = ();
	$ttlrows = 0;
	while (1) {
		foreach $i (@outlist) {
			$ary = collect_recs(31500);
			if (! $ary) {
				last;
			}
			$total += scalar(@$ary);
			$ttlrows += scalar(@$ary);
			@parm_ary = (1, $ary, { TYPE => SQL_VARBINARY, PRECISION => 32000 } );
			$rc = $sth[$i]->func(@parm_ary, BindParamArray);
			if (!defined($rc)) {
				print STDERR $sth[$i]->errstr;
				wrapup();
				return 1;
			}
			$rc = $sth[$i]->execute;
			if (!defined($rc)) {
				print STDERR $sth[$i]->errstr;
				wrapup();
				return 1;
			}
			$rc = $fastdbh[$i]->commit;
			if (!defined($rc)) {
				print STDERR $sth[$i]->errstr;
				wrapup();
				return 1;
			}
			$actives[$i] = 1;
		}
		if (! $ary) {
			last;
		}
#
#	wait for first available
#
		@parm_ary = (\@fastdbh);
		@outlist = $drh->func(@parm_ary, FirstAvailList);
		if (! @outlist) {
			print STDERR "Can't get a completed handle\n";
			sleep 1;
			next;
		}
		foreach $i (@outlist) {
			$rc = $sth[$i]->func(undef, Realize);
			if (!defined($rc)) {
				print STDERR $sth[$i]->errstr;
			}
			$actives[$i] = 0;
		}

		if ($total > $threshold) {
			print "$total rows loaded...\n";
			$threshold += 10000;
		}
		
		if (($ckpt != -1) && ($ttlrows >= $ckpt)) {
			print STDERR "Checkpointing...\n";
			$ttlrows = 0;
			for $i (0..(scalar(@actives)-1)) {
				if ($actives[$i] == 0) { next; }
				@parm_ary = (\@fastdbh);
				$i = $drh->func(@parm_ary, FirstAvailable);
				$sth[$i]->func(undef, Realize);
				$fastdbh[$i]->commit();
				$actives[$i] = 0;
			}
			$rc = $ctldbh->do('CHECKPOINT LOADING;');
			if (!defined($rc)) {
				print STDERR $ctldbh->errstr . "\n";
			}
		}
	}
#
#	wait for completions
#
	for $i (0..(scalar(@actives)-1)) {
		if ($actives[$i] == 0) { next; }
		@parm_ary = (\@fastdbh);
		$i = $drh->func(@parm_ary, FirstAvailable);
		$sth[$i]->func(undef, Realize);
		$fastdbh[$i]->commit();
		$actives[$i] = 0;
	}
	print "Data transferred, begin apply phase...\n";
	defined($ctldbh->do('CHECKPOINT LOADING END;')) || print STDERR $ctldbh->errstr . "\n";
	defined($ctldbh->commit()) || print STDERR $ctldbh->errstr . "\n";
	defined($ctldbh->do('END LOADING;')) || print STDERR $ctldbh->errstr . "\n";
	defined($ctldbh->commit()) || print STDERR $ctldbh->errstr . "\n";
		
	print "Apply phase complete.\n";
	
	$started = time - $started;
	$mins = int($started/60);
	$secs = $started%60;
	print "$total rows loaded in $mins:$secs...\n";
#
#	maybe retrieve the error records and report them here
#
	$ctldbh->{AutoCommit} = 1;
	$errsth = $ctldbh->prepare("SELECT COUNT(*) from fasttest_err1");
	my $errcnt = 0;
	$errsth->bind_col(1, \$errcnt);
	$errsth->execute;
	$errsth->fetch;
	if ($errcnt != 0) {
		print "$errcnt errors generated during fastload of fasttest.\n";
		print "Fastload completed. $total loaded.\n";
	}
	else {
		print "Fastload completed successfully. $total rows loaded.\n";
		print "Dropping error tables...\n";
		$ctldbh->do("DROP TABLE fasttest_err2");
		$ctldbh->do("DROP TABLE fasttest_err1");
	}
	return 1;
}

sub collect_recs { 
	my ($sz) = @_;
	
	my @ary = ();
	my $i = 0;
	my $s = 0;
	my $inbuf;
	my $rc = 0;
	while ($s < $sz) {
		if ($recfmt eq 'v') {
			$inbuf = <$inf>;
			@parms = split($vtsep, $inbuf);
			foreach $parm (@parms) {
				$ary[$i] .= pack('S A*', length($parm), $parm);
			}
		}
		elsif ($recfmt eq 'u') {
			$rc = sysread($inf, $inbuf, 2);
			if (! $rc) {
				if ($i > 0) { return \@ary; }
				return undef;
			}
			$len = unpack('S', $inbuf);
			if ($len + $s + 4 > $sz) {
				sysseek($inf, -2, SEEK_CUR);
				return \@ary;
			}
			$rc = sysread($inf, $inbuf, $len);
			if (! $rc) {
				if ($i > 0) { return \@ary; }
				return undef;
			}
			$ary[$i] = pack('S a* c', $len, $inbuf, 10);
		}
		elsif ($recfmt eq 'f') {
			$rc = sysread($inf, $inbuf, 2);
			if (! $rc) {
				if ($i > 0) { return \@ary; }
				return undef;
			}
			$len = unpack('S', $inbuf);
			if ($len + $s + 4 > $sz) {
				sysseek($inf, -2, SEEK_CUR);
				return \@ary;
			}
			$rc = sysread($inf, $inbuf, $len+1);
			if (! $rc) {
				if ($i > 0) { return \@ary; }
				return undef;
			}
			$ary[$i] = pack('S a*', $len, $inbuf);
		}
		$s += length($ary[$i]) + 1;
		$i++;
	}
	return \@ary;
}
	
sub wrapup {
	$ctldbh->do('CHECKPOINT LOADING END;');
	$ctldbh->commit();
	$ctldbh->do('END LOADING;');
	$ctldbh->commit();
	
	$drh->disconnect_all;
	return 1;
}
