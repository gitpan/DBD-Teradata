#!/usr/bin/perl
#
#	fastexp.pl - script to emulate FASTEXPORT command utility
#
#	Coded 12/9/2000	by Dean Arnold
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
	'DATEFORM', \&dateform,			# done
	'EXPORT', \&def_export,			# done
	'FIELD', \&def_field,			# done
	'FILLER', \&def_filler,			# done
	'IF', \&if_cmd,					# done
	'ELSE', \&else_cmd,				# done
	'ENDIF', \&endif_cmd,			# done
	'IMPORT', \&def_import,			# done
	'LAYOUT', \&def_layout,			# done
	'LOGOFF', \&logoff,				# done
	'LOGON', \&logon,				# done
	'LOGTABLE', \&def_logtable,		# done
	'SET', \&set_cmd,				# done
	'SYSTEM', \&os_cmd,				# done
	'ACCEPT', \&nosupp,				# done
	'DISPLAY', \&nosupp,			# done
);

%cmds2 = (
	'BEGIN EXPORT', \&begin_export,	# done
	'END EXPORT', \&end_export,		# done
	'ROUTE MESSAGE', \&nosupp,		# done
	'RUN FILE', \&nosupp			# done
);

%logon1st = (
	'BEGIN EXPORT', 1,
	'END EXPORT', 1
);

my @declens = ( 0, 1, 1, 2, 2, 4, 4, 4, 4, 4, 8, 8, 8, 8, 8, 8, 8, 8, 8);
my %typelen = ( 'INT', 4, 'INTEGER', 4, 'BYTEINT', 1, 'SMALLINT', 2, 'FLOAT', 8, 
	'DATE', 4, 'LONG VARCHAR', 32000, 'LONG VARBYTE', 32000);
my %typeabr = ( 'INT', 'INTEGER', 'DEC', 'DECIMAL', 'CHARACTER', 'CHAR',
	'CHARACTERS', 'CHAR' );

my $numsess = 0;
my $drh;			# driver handle
my @fastdbh = ();	# array of fastexp sessions
my $ctldbh;			# control session
my $errdbh;			# error session
my $exporting = 0;	# 1 => BEGIN LOADING issued
my $sel_stmt = '';	# SELECT statement for export
my $using = '';		# USING clause generated from DEFINE
my $ttlrows = 0;	# total rows transfered
my $i;
my $remnant = '';
my $first = '';
my $second = '';
my @fielddefs = ();	# DEFINE'd input fields
my $indics = 0;		# 1 => use indicators
my $dateformat = 0; # 1 => use ANSI, else integer
my $startrec = 0;	# first record to load, starting from 0
my $endrec = undef;	# last record to load, undef means all
my $vtsep = '|';
my $vtdisp = 0;
my $vtnostop = 0;
my $recfmt = 'FASTLOAD';	# output format
my $inrecfmt = 'FASTLOAD';	# input format
my $maxses = 24;	# undef => logon to all AMPs
my $minses = 1;		# min number of session to logon
my $infilenm = '';	# input data file name
my @fldname = ();	# input data field definitions
my %fldtype = ();
my %fldprec = ();
my %fldnullif = ();
my %fldpos = ();
my $outf;			# output data file handle
my $table = '';
my $logtable = '';
my $logtable_ct = 0;	# 1 => logtable has been created
my $run_file = 0;	# 1 => command file input
my $layoutnm = '';
my @fillpos = ();
my @filltype = ();
my @fillprec = ();
my $ifcnt = 0;		# incremented for each IF, decremented for each ENDIF
my $fileout = '';	# EXPORT output filename
my $file_in = '';	# command input filename
my %variables = ();
my $outlimit = -1;	# max records to export; -1 means all

print "Enter FASTEXPORT command:\n";
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
	if (($_ eq '') || ($_=~/^\s*$/)) {
		next;	# ignore empty input
	}
	if ($incmd == 0) {
		if ($_=~/^\s*\./) { 
			$dotcmd = 1;
			$_=~s/^\s*\.//;
		}
		else { $dotcmd = 0; }
		$cmd = $_;
	}
	else {
		$cmd .= "\n" . $_;
	}
	if ($cmd!~/;\s*$/) { 
		$incmd = 1;
		next;
	}
#
#	got a complete command, process it
#
	print "\n$cmd\n";
	$incmd = 0;
	$cmd =~s/;\s*$//;
	$cmd =~s/\n/ /g;
	
	if (!$dotcmd) {
		if (!$ctldbh) {
			print STDERR "You must be logged on to do that.\n";
			print "Enter FASTEXPORT command:\n";
			next;
		}
		execsql($cmd);
		print "Enter FASTEXPORT command:\n";
		next;
	}
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
	else { 
		print "Enter FASTEXPORT command:\n";
		next; 
	}
	
	if (($ifcnt != 0) && ($cmd!~/^(IF|ELSE|ENDIF)\s/i)) {
		print "Enter FASTEXPORT command:\n";
		next;
	}
	
	$remnant=~s/^\s+//;
	$remnant=~s/\s+$//;
	
	my $handler;
	if (defined($cmds2{($first . ' ' . $second)})) {
		if ((!defined($ctldbh)) && defined($logon1st{($first . ' ' . $second)})) {
			print STDERR "You must be logged on to do that.\n";
			print "Enter FASTEXPORT command:\n";
			next;
		}
		$handler = $cmds2{($first . ' ' . $second)};
		&$handler(($first . ' ' . $second), $remnant);
	}
	elsif (defined($cmds{$first})) {
		if ((!defined($ctldbh)) && defined($logon1st{$first})) {
			print STDERR "You must be logged on to do that.\n";
			print "Enter FASTEXPORT command:\n";
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
	print "Enter FASTEXPORT command:\n";
}

print "End of input, exitting...";
wrapup();

sub def_logtable {
	my ($cmd, $remnant) = @_;
	
	if ($remnant=~/^(\S+)(.*)$/) {
		$logtable = $1;
		if ($2 ne '') {
			print STDERR "Extra text following LOGTABLE command ignored.\n";
		}
	}

	if (defined($ctldbh)) {
#
#	Create logtable:
#
		$ctldbh->do('DROP TABLE $logtable') || print STDERR ($ctldbh->errstr . "\n");
		my $rc = $ctldbh->do(
		'CT $logtable, FALLBACK (' .
		'	LogType int,' .
		'	Seq int,' .
		'	ReqRC int,' .
		'	ReqType int,' .
		'	ReqLen  int,' .
		'	ReqMsg  varchar(255),' .
		'	SysInfo varbyte(255),' .
		'	MiscInt1 int,' .
		'	MiscInt2 int,' .
		'	MiscInt3 int,' .
		'	MiscInt4 int,' .
		'	MiscInt5 int,' .
		'	MiscInt6 int,' .
		'	MiscInt7 int,' .
		'	MiscInt8 int, ' .
		'	FExptSeq int default 0,' .
		'	FExptImpSeq int,' .
		'	FExptSrcSeq int, ' .
		'	FExptCkpt varbyte(255),' . 
		'	RunDate date default DATE,' .
		'	RunTime float default TIME' .
		') UNIQUE PRIMARY INDEX (LogType, Seq, FExptSeq)');
		if (!defined($rc)) {
			print STDERR "Unable to create logtable $logtable:\n" . $ctldbh->errstr . "\n";
			$logtable_ct = 0;
		}
		$logtable_ct = 1;
	}
	return 1;
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
#	logon control session only; others are logged on
#	at BEGIN EXPORT
#
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
	$lsn = $ctldbh->{'tdat_lsn'};
	$drh = $ctldbh->{'Driver'};
	
	if ($logtable ne '') {
#
#	Create logtable:
#
		$ctldbh->do('DROP TABLE $logtable') || print STDERR ($ctldbh->errstr . "\n");
		my $rc = $ctldbh->do(
		'CT $logtable, FALLBACK (' .
		'	LogType int,' .
		'	Seq int,' .
		'	ReqRC int,' .
		'	ReqType int,' .
		'	ReqLen  int,' .
		'	ReqMsg  varchar(255),' .
		'	SysInfo varbyte(255),' .
		'	MiscInt1 int,' .
		'	MiscInt2 int,' .
		'	MiscInt3 int,' .
		'	MiscInt4 int,' .
		'	MiscInt5 int,' .
		'	MiscInt6 int,' .
		'	MiscInt7 int,' .
		'	MiscInt8 int, ' .
		'	FExptSeq int default 0,' .
		'	FExptImpSeq int,' .
		'	FExptSrcSeq int, ' .
		'	FExptCkpt varbyte(255),' . 
		'	RunDate date default DATE,' .
		'	RunTime float default TIME' .
		') UNIQUE PRIMARY INDEX (LogType, Seq, FExptSeq)');
		if (!defined($rc)) {
			print STDERR "Unable to create logtable $logtable:\n" . $ctldbh->errstr . "\n";
			$logtable_ct = 0;
		}
		$logtable_ct = 1;
	}
	return 1;
}

sub nosupp {
	my ($cmd, $remnant) = @_;
	
	print STDERR "$cmd not supported in this release.\n";
	return 1;
}

sub logoff {
	my ($cmd, $remnant) = @_;
	my $rc = 0;

	if ($remnant=~/^(\d+)$/) {
		$rc = $1;
	}
	elsif ($remnant ne '') {
		$rc = 0;
		print STDERR "Extraneous command text ignored.\n";
	}
#
#	logoff fastexp sessions
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
	
	if (defined($errdbh)) { $errdbh->disconnect; }
	$ctldbh->disconnect;
	undef $errdbh;
	undef $ctldbh;
	print STDERR "Logged off.\n";
	exit $rc;
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

sub def_layout {
	my ($cmd, $remnant) = @_;
	$layoutnm = '';
	if ($remnant=~/^(\S+)\s+(.*)$/) {
		$layoutnm = $1;
		$remnant = ' ' . $2 . ' ';
		if ($remnant=~/\sINDICATORS\s/i) {
			$indics = 1;
		}
		else {
			$indics = 0;
		}
		if ($remnant=~/\sCONTINUEIF\s/i) {
			print STDERR "CONTINUEIF not supported in this release.\n";
		}
	}
	else {
		print STDERR "No layout name provided.\n";
	}
	return 1;
}

sub if_cmd {
	my ($cmd, $remnant) = @_;
	print STDERR "IF/ELSE/ENDIF not supported in this release. Skipping ahead to ENDIF.\n";
	$ifcnt++;
	return 1;
}

sub else_cmd {
	my ($cmd, $remnant) = @_;
	if ($ifcnt != 0) {
		print STDERR "IF/ELSE/ENDIF not supported in this release. Skipping ahead to ENDIF.\n";
	}
	else {
		print STDERR "ELSE without prior IF. Skipping ahead to ENDIF.\n";
		$ifcnt++;
	}
	return 1;
}

sub endif_cmd {
	my ($cmd, $remnant) = @_;
	if ($ifcnt == 0) {
		print STDERR "ENDIF without prior IF ignored.\n";
	}
	else {
		$ifcnt--;
		if ($ifcnt == 0) {
			print STDERR "ENDIF found, processing continues.\n";
		}
	}
	return 1;
}

sub def_export {
	my ($cmd, $remnant) = @_;

	my ($type, $name, $mode);
	$remnant = ' ' . $remnant . ' ';
	if ($remnant=~/\sOUTMOD\s/i) {
		print STDERR "OUTMOD not supported in this release.\n";
		return 1;
	}
	if ($remnant=~/\sAXSMOD\s+/i) {
		print STDERR "AXSMOD not supported in this release.\n";
		return 1;
	}
	if ($remnant=~/\sBLOCKSIZE\s/i) {
		print STDERR "BLOCKSIZE not supported in this release; default 32K blocksize used.\n";
	}
	if ($remnant=~/\sMLSCRIPT\s/i) {
		print STDERR "$1 not supported in this release; ignored.\n";
	}
	if ($remnant!~/^\s+OUTFILE\s+(\S+)/i) {
		print STDERR "Invalid EXPORT definition.\n";
		return 1;
	}
	$name = $1;
	$indicout = 1;
	if ($remnant=~/\sMODE\s+(\S+)\s/i) {
		$mode = $1;
		if ($mode!~/^(INDICATORS|RECORD)$/i) {
			print STDERR "Invalid mode $mode; INDICATORS assumed.\n";
		}
		elsif ($mode=~/^RECORD$/i) { $indicout = 0; }
	}
	if ($remnant=~/\sFORMAT\s+(\S+)\s/i) {
		$mode = $1;
		if ($mode!~/^(FASTLOAD|BINARY|TEXT|UNFORMAT)$/i) {
			print STDERR "Invalid format $mode; FASTLOAD assumed.\n";
		}
		else { 
			$recfmt = $mode; 
		}
	}
	if ($remnant=~/\sOUTLIMIT\s+(\S+)\s/i) {
		$mode = $1;
		if ($mode!~/^\d+$/i) {
			print STDERR "Non-numeric OUTLIMIT specified; OUTLIMIT ignored.\n";
			$outlimit = -1;
		}
		else { 
			$outlimit = $mode;
		}
	}
	$fileout = $name;
	return 1;
}

sub def_import {
	my ($cmd, $remnant) = @_;

	my ($type, $name, $mode, $tmp);
	if ($remnant!~/^(INFILE|INMOD)\s+(\S+)\s+(.*)$/i) {
		print STDERR "Invalid IMPORT definition.\n";
		return 1;
	}
	$type = $1; $name = $2; $remnant = $3;
	if ($type=~/INMOD/i) {
		print STDERR "INMOD not supported in this release.\n";
		return 1;
	}
	if ($remnant=~/^AXSMOD\s+/i) {
		print STDERR "AXSMOD not supported in this release.\n";
		return 1;
	}
	$remnant = ' ' . $remnant . ' ';
	if ($remnant=~/\sLAYOUT\s+(\S+)\s/i) {
		$tmp = $1;
		if ($layoutnm ne $tmp) {
			print STDERR "Undefined layout $inlayout; IMPORT ignored.\n";
			return 1;
		}
	}
	$indicout = 1;
	if ($remnant=~/\sFORMAT\s+(\S+)\s/i) {
		$mode = $1;
		if ($mode!~/^(FASTLOAD|BINARY|TEXT|UNFORMAT|VARTEXT)$/i) {
			print STDERR "Invalid format $mode; FASTLOAD assumed.\n";
		}
		else { 
			$inrecfmt = $mode; 
			if (($inrecfmt eq 'VARTEXT') &&
				($remnant=~/\sFORMAT\s+VARTEXT\s+(.+)\s+LAYOUT/i)) {
				$mode = $1;
				if ($mode=~/DISPLAY\s+ERRORS/) { $vtdisp = 1; }
				if ($mode=~/NOSTOP/) { $vtnostop = 1; }
				if ($mode=~/\'(.)\'/) { $vtsep = $1; }
			}
		}
	}
	$infilenm = $name;
	$inlayout = $tmp;
	return 1;
}

sub def_field {
	my ($cmd, $remnant) = @_;
#
#	now collect the fields and types
#
	if ($layoutnm eq '') {
		print STDERR "No LAYOUT currently defined.\n";
		return 1;
	}
	my ($name, $pos, $prec, $type);
	if ($remnant!~/^(\S+)\s+/) {
		print STDERR "No fieldname given.\n";
		return 1;
	}
	$name = $1;
	$remnant=~s/^(\S+)\s+//;
	$remnant = ' ' . $remnant . ' ';
	if ($remnant=~/\sDROP\s+/i) {
		print STDERR "DROP field qualifier not supported in this release.\n";
		return 1;
	}
	if ($remnant=~/\sNULLIF\s+/i) {
		print STDERR "NULLIF field qualifier not supported in this release.\n";
		return 1;
	}
	if ($remnant=~/^(\d+)\s+(.+)$/) {
		$pos = $1;
		$remnant = $2;
	}
	elsif ($remnant=~/^\*\s+(.+)$/) {
		$pos = '*';
		$remnant = $2;
	}
	elsif ($remnant=~/^\S+\s*\|\|/) {
		print STDERR "Field concatenation not supported in this release.\n";
		return 1;
	}
	else {
		print STDERR "Invalid FIELD command.\n";
		return 1;
	}
	if ($remnant=~/^\s*(BYTE|VARBYTE|CHAR|VARCHAR|CHARACTERS|CHARACTER|DECIMAL|DEC|GRAPHIC)\s*\(([^\)]+)\)/i) {
		$type = uc $2; $prec = $3;
	}
	elsif ($remnant=~/^\s*(BYTEINT|SMALLINT|INT|INTEGER|DATE|FLOAT)/i) {
		$type = uc $2; $prec = '';
	}
	elsif ($remnant=~/^\s*LONG\s+(VARCHAR|VARBYTE)/i) {
		$type = 'LONG ' . (uc $2); $prec = '';
	}
	elsif ($remnant=~/^\s*LONG\s+(VARCHAR|VARBYTE)/i) {
		$type = 'LONG ' . (uc $2); $prec = '';
	}
	else {
		print STDERR "Unrecognized field type definition.\n";
		return 1;
	}

	if (($prec=~/,/) && ($type!~/^DEC(IMAL)?$/)) {
		print STDERR "Invalid precision for type $type.\n";
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
		print STDERR "Field $name previously defined; current definition ignored.\n";
		return 1;
	}
	push(@fldname, $name);
	if (defined($typeabr{$type})) { $type = $typeabr{$type}; }
	$fldtype{$name} = $type;
	$fldprec{$name} = $prec;
	$fldpos{$name} = $pos;
	return 1;
}

sub def_filler {
	my ($cmd, $remnant) = @_;
#
#	now collect the fields and types
#
	if ($layoutnm eq '') {
		print STDERR "No LAYOUT currently defined.\n";
		return 1;
	}
	my ($name, $pos, $prec, $type);
	if ($remnant=~/^([A-Za-z_][\w+])\s+/) {
		$name = $1;
		$remnant=~s/^[A-Za-z_][\w+]\s+(.*)$/$2/;
	}
	else { $name = ''; }
	if ($remnant=~/^(\d+)\s+(.+)$/) {
		$pos = $1;
		$remnant = $2;
	}
	elsif ($remnant=~/^\*\s+(.+)$/) {
		$pos = '*';
		$remnant = $2;
	}
	else {
		print STDERR "No position specified for FILLER command.\n";
		return 1;
	}
	if ($remnant=~/^\s*(BYTE|VARBYTE|CHAR|VARCHAR|CHARACTERS|CHARACTER|DECIMAL|DEC|GRAPHIC)\s*\(([^\)]+)\)/i) {
		$type = uc $2; $prec = $3;
	}
	elsif ($remnant=~/^\s*(BYTEINT|SMALLINT|INT|INTEGER|DATE|FLOAT)/i) {
		$type = uc $2; $prec = '';
	}
	elsif ($remnant=~/^\s*LONG\s+(VARCHAR|VARBYTE)/i) {
		$type = 'LONG ' . (uc $2); $prec = '';
	}
	elsif ($remnant=~/^\s*LONG\s+(VARCHAR|VARBYTE)/i) {
		$type = 'LONG ' . (uc $2); $prec = '';
	}
	else {
		print STDERR "Unrecognized FILLER type definition.\n";
		return 1;
	}

	if (($prec=~/,/) && ($type!~/^DEC(IMAL)?$/)) {
		print STDERR "Invalid precision for type $type.\n";
		return 1;
	}
	if ($type=~/^DEC(IMAL)?$/) {
		if (($prec=~/^\s*(\d+)/) && ($1 > 18)) {
			print STDERR "Precision too large for FILLER.n";
			return 1;
		}
		elsif (($prec=~/^\s*(\d+)\s*,\s*(\d+)/) && ($1 < $2)) {
			print STDERR "Scale too large for FILLER.\n";
			return 1;
		}
	}
	if (defined($fldtype{$name})) {
		print STDERR "FILLER $name previously defined; current definition ignored.\n";
		return 1;
	}
	push(@fillpos, $pos);
	if (defined($typeabr{$type})) { $type = $typeabr{$type}; }
	push(@filltype,$type);
	push(@fillprec, $prec);
	return 1;
}

sub os_cmd {
	my ($cmd, $remnant) = @_;
	system($remnant);
	return 1;
}

sub set_cmd {
	my ($cmd, $remnant) = @_;
	
	if ($remnant=~/^(\S+)\s+(TO\s+)?(.+)$/i) {
		$var = $1; $expr = $3;
		$variables{$var} = $expr;
	}
	else {
		print STDERR "Invalid SET command ignored.\n";
	}
	return 1;
}

sub begin_export {
	my ($cmd, $remnant) = @_;
	$remnant .= ' ';
	$remnant = ' ' . $remnant;
	if ($remnant=~/\sSLEEP\s/i) {
		print STDERR "SLEEP not supported in this release; ignored.\n"
	}
	if ($remnant=~/\sTENACITY\s/i) {
		print STDERR "TENACITY not supported in this release; ignored.\n"
	}
	if ($remnant=~/\sNOTIFY\s/i) {
		print STDERR "NOTIFY not supported in this release; ignored.\n"
	}
	if (! $logtable_ct) {
		print STDERR "LOGTABLE must be specified first!.\n";
		return 1;
	}
	$maxses = 24;
	$minses = 1;
	if ($remnant=~/\sSESSIONS\s+(\d+)\s+(\d+)\s/i) {
		$maxses = $1; $minses = $2;
	}
	elsif ($remnant=~/\sSESSIONS\s+(\d+)\s+\*\s/i) {
		$maxses = $1; $minses = 1;
	}
	elsif ($remnant=~/\sSESSIONS\s+\*\s+(\d+)\s/i) {
		$maxses = 24; $minses = $1;
	}
	elsif ($remnant=~/\sSESSIONS\s\*\s+\*\s/i) {
		$maxses = 24; $minses = 1;
	}
	elsif ($remnant=~/\sSESSIONS\s+(\d+)\s/i) {
		$maxses = $1; $minses = 1;
	}
	elsif ($remnant=~/\sSESSIONS\s+\*\s/i) {
		$maxses = 24; $minses = 1;
	}
	elsif ($remnant=~/\sSESSIONS\s+/i) {
		print STDERR "Invalid SESSIONS command.\n";
		return 1;
	}
	if (($minses > 24) || ($maxses > 24)) {
		print STDERR "Max sessions currently limited to 24.\n";
		$maxses = 24; $minses = 1;
	}
	elsif ($minses <= 0) {
		print STDERR "Invalid minimum SESSIONS specified; assuming 1.\n";
		$minses = 1;
	}
	elsif ($maxses <= 0) {
		print STDERR "Invalid maximum SESSIONS specified; assuming 24.\n";
		$minses = 1; $maxses = 24;
	}
	elsif ($maxses < $minses) {
		print STDERR "Invalid SESSIONS specified; min is greater than max. Assuming max=24 and min=1.\n";
		$maxses = 24; $minses = 1;
	}
#
#	now logon the error and export sessions
#
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
			tdat_utility => EXPORT
			});
		if (!defined($dbh)) {
			last;
		}
		$fastdbh[$i] = $dbh;
	}
	$i = scalar(@fastdbh);
	if ($i < $minses) {
		print STDERR ('Failure ' . $DBI::err . ': ' . $DBI::errstr . "\n"); 
		print STDERR "Cannot logon minimum number of EXPORT sessions.\n";
		print STDERR "Requested minimum was $minses, but only got $i.\n";
		foreach my $dbh (@fastdbh) {
			$dbh->disconnect;
		}
		@fastdbh = ();
		$errdbh->disconnect;
		undef $errdbh;
		return 1;
	}
	if ($DBI::err) { print STDERR ($DBI::errstr . "\n"); }
	print STDERR "$i EXPORT sessions logged on.\n";
	$exporting = 1;
	return 1;
}

sub end_export {
	my ($cmd, $remnant) = @_;

	$rc = $ctldbh->do('END FASTEXPORT');
	if (!defined($rc)) {
		print STDERR $ctldbh->errstr . "\n" ;
	}
	$rc = $ctldbh->commit();
	if (!defined($rc)) {
		print STDERR $ctldbh->errstr . "\n";
	}
	$exporting = 0;
	foreach my $dbh (@fastdbh) {
		$dbh->disconnect;
	}
	@fastdbh = ();
	$errdbh->disconnect;
	undef $errdbh;
	return 1;
}

sub versions {
	my ($cmd, $remnant) = @_;
	print STDERR "fastexp.pl ver. $VERSION\n";
	return 1;
}

sub execsql {
	my ($cmd, $remnant) = @_;
	if (!$exporting) { 
		my $rc = $ctldbh->do($cmd);
		if (!defined($rc)) {
			print STDERR $ctldbh->errstr , "\n";
		}
		return 1;
	}
	my $stmt = $cmd;
	if ($cmd!~/^\s*(SEL|SELECT|LOCK|LOCKING)/i) {
		print STDERR "Only SELECT statement allowed after BEGIN EXPORT.\n";
		return 1;
	}
	if ($cmd=~/^\s*(LOCK|LOCKING)\s+(TABLE|DATABASE|VIEW)\s+\S+\s+(FOR |IN )?\s*(ACCESS|EXCLUSIVE|EXCL|SHARE|READ|WRITE)\s+(MODE |NOWAIT )?\s*(.+)$/i) {
		my $tmp = $6;
		if ($tmp!~/^(SEL|SELECT)\s/i) {
			print STDERR "Only SELECT statement allowed after BEGIN EXPORT.\n";
			return 1;
		}
	}
#
#	open input and output files
#
	if ($fileout eq '') {
		print STDERR "No output data file specified.\n";
		return 1;
	}
	if (! open($outf, ">$fileout")) {
		print STDERR "Unable to open output file $fileout: $!\n";
		return 1;
	}
	if ($recfmt ne 'TEXT') {
		binmode $outf;
	}
	
	if ($infilenm ne '') {
		if (! open($inf, "<$infilenm")) {
			print STDERR "Unable to open import file $infilenm: $!\n";
			return 1;
		}
		if ($inrecfmt ne 'VARTEXT') {
			binmode $inf;
		}
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
	}
	else { 
		undef $inf; 
		$using = '';
	}
#
#	build USING clause (if any)
#
	$stmt = $using . ' ' . $stmt;
#
#	prepare the statement
#
	my $expsth = $ctldbh->prepare($stmt, { tdat_keepresp => 1 });
	if (! $expsth) {
		print STDERR ('Unable to prepare query: ' . $ctldbh->errstr . "\n");
		return 1;
	}
	my $types = $expsth->{TYPE};
	my $precs = $expsth->{PRECISION};
	my $scales = $expsth->{SCALE};
#
#	NULL Select from logtable:
#
	my $rc = $ctldbh->do('Select NULL from $logtable where (LogType = 220) and (Seq = 1) and (FExptSeq = 0)');
	if (!defined($rc)) {
		print STDERR ('Null SELECT on $logtable failed: ' . $ctldbh->errstr . "\n");
		return 1;
		$expsth->finish;
	}
#
#	prepare the log stmts now, since we can't prepare after BEGIN EXPORT
#
	my $log1sth = $errdbh->prepare(
	'SELECT MiscInt1 (INTEGER), MiscInt2 (INTEGER), FExptSeq (INTEGER), FExptCkpt (VARBYTE(255)) ' .
	'FROM fexptst_log WHERE (LogType = 210) and (Seq = 1) and ' .
	'(FExptSeq IN (SELECT MAX(FExptSeq) FROM fexptst_log where (LogType = 210) and (Seq = 1)))');
	if (! $log1sth) {
		print STDERR ('1st log query failed: ' . $errdbh->errstr . "\nExitting...\n");
		$drh->disconnect_all;
		exit;
	}
	
	my $log2sth = $errdbh->prepare(
	'SELECT MiscInt1(INTEGER), MiscInt2(INTEGER), MiscInt3(INTEGER), FExptSeq(INTEGER), FExptCkpt(VARBYTE(255)) ' .
	'FROM fexptst_log WHERE (LogType = 212) and (Seq = 1) and ' . 
	'(FExptSeq IN (SELECT MAX(FExptSeq) from fexptst_log where (LogType = 212) and (Seq = 1)))');

	if (! $log2sth) {
		print STDERR ('2nd log query failed: ' . $errdbh->errstr . "\nExitting...\n");
		$drh->disconnect_all;
		exit;
	}
	
	my $endsth = $errdbh->prepare("INS fexptst_log (LogType, Seq) VALUES(220, 1);");
	if (! $endsth) {
		print STDERR ('End log insert failed: ' . $errdbh->errstr . "\nExitting...\n");
		$drh->disconnect_all;
		exit;
	}
#
#	send BT;BEGIN FASTEXPORT; on control session
#
	$ctldbh->{AutoCommit} = 0;
	$rc = $ctldbh->do('BEGIN FASTEXPORT;');
	if (!defined($rc)) {
		print STDERR ($ctldbh->errstr . "\n");
	}
#	
# execute on error session:
#
	my ($miscint1, $miscint2, $miscint3, $fexptseq, $fexptckpt);
	$log1sth->bind_col(1, \$miscint1);
	$log1sth->bind_col(2, \$miscint2);
	$log1sth->bind_col(3, \$fexptseq);
	$log1sth->bind_col(4, \$fexptckpt);
	$rc = $log1sth->execute;
	if (! $rc) {
		print STDERR ('1st log query failed: ' . $log1sth->errstr . "\n");
		$rc = $ctldbh->do('END FASTEXPORT');
		$ctldbh->commit();
		return 1;
	}
	while (	$log1sth->fetch ) {
		print "MiscInt1: $miscint1 MiscInt2: $miscint2 FexptSeq: $fexptseq FexptCkpt: $fexptckpt\n";
	}
	$log1sth->finish;
#
#	execute on error session:
#
	$log2sth->bind_col(1, \$miscint1);
	$log2sth->bind_col(2, \$miscint2);
	$log2sth->bind_col(3, \$miscint3);
	$log2sth->bind_col(4, \$fexptseq);
	$log2sth->bind_col(5, \$fexptckpt);
	$rc = $log2sth->execute;
	if (! $rc) {
		print STDERR ('2nd log query failed: ' . $log1sth->errstr . "\n");
		$rc = $ctldbh->do('END FASTEXPORT');
		$ctldbh->commit();
		return 1;
	}
	while (	$log2sth->fetch ) {
		print STDERR "MiscInt1: $miscint1 MiscInt2: $miscint2 MiscInt3: $miscint3 FexptSeq: $fexptseq FexptCkpt: $fexptckpt\n";
	}
	$log2sth->finish;
#
#	Execute fastexport query on control session with KEEPRESP parcel.
#			 =>DBMS returns successparcel, endreq parcel
#
	my $started = time;
	
	$rc = $expsth->execute;
	if (! $rc) {
		print STDERR ('EXPORT Query execute failed: ' . $expsth->errstr . "\n");
		$rc = $ctldbh->do('END FASTEXPORT');
		$ctldbh->commit();
		return 1;
	}
#
#	clone export statement context on each EXPORT session
#		 
	my @sths = ();
	my $seqnum = 1;
	my $qnum = 1;
	my @outlist = (0);
	my @ary = ();
	my @parm_ary = (1, \@ary, 1000);
	my $total = 0;	
	my $threshold = 10000;
	my $activecnt = 0;
	foreach $i (0..scalar(@fastdbh)-1) {
		$sths[$i] = $fastdbh[$i]->prepare(';', {
			tdat_nowait => 1,
			tdat_raw => (($indics == 0) ? RecordMode : IndicatorMode),
			tdat_clone => $expsth	# inherit attributes from this statement
		});
		if (!defined($sths[$i])) {
			print STDERR $fastdbh[$i]->errstr . "\n";
			next;
		}
#
#	2 params need to be bound: the query number, and the sequence number
#
		$sths[$i]->bind_param_inout(1, \$qnum, SQL_INTEGER);
		$sths[$i]->bind_param_inout(2, \$seqnum, SQL_INTEGER);
		$sths[$i]->func(@parm_ary, BindColArray);

		$sths[$i]->execute;
		$activecnt++;
		$seqnum++;
	}

	while ($activecnt > 0) {
		@parm_ary = (\@fastdbh);
		@outlist = $drh->func(@parm_ary, FirstAvailList);
		if (! @outlist) {
			print STDERR "Can't get a completed handle\n";
			sleep 1;
			next;
		}
		foreach $i (@outlist) {
			$rc = $sths[$i]->func(undef, Realize);
			if (!defined($rc)) {
				$activecnt--;
				if ($sths[$i]->err != 2588) {
					print STDERR $sths[$i]->errstr . "\n";
				}
				$sths[$i]->finish;
			}
			else {
				while ($sths[$i]->fetch) {
					$total += scalar(@ary);
					foreach my $row (@ary) {
						print $outf $row;
					}
					if ($total >= $threshold) {
						print STDERR "Got $total rows...\n";
						$threshold += 10000;
					}
					@ary = ();
				}
				$seqnum++;
				$rc = $sths[$i]->execute;
			}
		}
	}
	$started = time - $started;
	$mins = int($started/60);
	$secs = $started%60;
	print "$total rows exported in $mins:$secs...\n";
#
#	finish up the EXPORT
#
	$endsth->execute;
	close $outf;
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
	$ctldbh->do('END FASTEXPORT;');
	$ctldbh->commit();
	
	$drh->disconnect_all;
	return 1;
}
