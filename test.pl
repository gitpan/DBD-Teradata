use DBI;
use DBI qw(:sql_types);
use FileHandle;

my $alphas = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_ ';
my %typestr = (
	SQL_VARCHAR, 'VARCHAR', 
	SQL_CHAR, 'CHAR', 
	SQL_FLOAT, 'FLOAT', 
	SQL_DECIMAL, 'DECIMAL',
	SQL_INTEGER, 'INTEGER', 
	SQL_SMALLINT, 'SMALLINT', 
	SQL_TINYINT, 'TINYINT', 
	SQL_VARBINARY, 'VARBINARY',
	SQL_BINARY, 'BINARY',
	SQL_LONGVARBINARY, 'LONG VARBINARY',
	SQL_DATE, 'DATE',
	SQL_TIMESTAMP, 'TIMESTAMP',
	SQL_TIME, 'TIME'
	);

my $dbh;

my $dsn = $ENV{'TDAT_DBD_DSN'};
my $userid = $ENV{'TDAT_DBD_USER'};
my $passwd = $ENV{'TDAT_DBD_PASSWORD'};
if (!defined($dsn)) {
	die "No host defined...check TDAT_DBD_DSN environment variable\n";
}
if (!defined($userid)) {
	die "No userid defined...check TDAT_DBD_USER environment variable\n";
}
if (!defined($passwd)) {
	die "No password defined...check TDAT_DBD_PASSWORD environment variable\n";
}

warn "Logging onto $dsn as $userid...\n";
$dbh = DBI->connect("dbi:Teradata:$dsn", $userid, $passwd,
	{
		PrintError => 0,
		RaiseError => 0
	}
) || die "Can't connect to $dsn: $DBI::errstr. Exitting...\n";
warn "Logon ok.\n";
my $drh = $dbh->{Driver};
warn "DBD::Teradata v. $drh->{Version}\n";
#
#	test DDL
#
warn "Testing DDL...\n";
$dbh->do( 'DROP TABLE alltypetst');
if ($dbh->err != 0) { 
	($dbh->err != 3807) ? die $dbh->errstr : warn $dbh->errstr . "\n" ;
}

my $ctsth = $dbh->prepare( 'CREATE TABLE alltypetst, NO FALLBACK ('
. 'col1 integer, col2 smallint, col3 byteint, col4 char(20), '
. 'col5 varchar(100), col6 float, col7 decimal(2,1), '
. 'col8 decimal(4,2), col9 decimal(8,4), col10 decimal(14,5), '
. 'col11 date, col12 time, col13 timestamp(0)) unique primary index(col1);'
) || die ($dbh->errstr . "\n");

defined($ctsth->execute) || die ($ctsth->errstr . "\n");
$ctsth->finish || die ($ctsth->errstr . "\n");

my $sth = $dbh->prepare('SHOW TABLE alltypetst') || die ($dbh->errstr . "\n");
defined($sth->execute) || die ($sth->errstr . "\n");
my $names = $sth->{NAME};

while (@row = $sth->fetchrow_array() ) {
	for (my $field = 0; $field < scalar(@row); $field++) {
		
		if (defined($row[$field])) {
			$row[$field]=~s/\r/\n/g;
			print "$$names[$field]:\n$row[$field]\n";
		}
		else {
			print "$$names[$field]: NULL\n";
		}
	}
	print "\n";
}
$sth->finish || die ($sth->errstr . "\n");

#
#	HELP TABLE does not return proper data for
#	the max and min range fields; it should be 8 bytes each,
#	and it appears to return 9 bytes ???
#
$sth = $dbh->prepare('EXPLAIN select * from alltypetst') || die ($dbh->errstr . "\n");
defined($sth->execute) || die ($sth->errstr . "\n");

while (@row = $sth->fetchrow_array() ) {
	for (my $field = 0; $field < scalar(@row); $field++) {
		if (defined($row[$field])) {
			$row[$field]=~s/\r/\n/g;
			print "$row[$field]";
		}
		else {
			print "NULL\n";
		}
	}
	print "\n";
}
$sth->finish;

warn "DROP/CREATE/SHOW table and EXPLAIN ok.\n";
#
#	test MACRO execution
#
warn "Testing Macro creation...\n";

my $dmsth = $dbh->prepare( 'DROP MACRO dbitest');
if ($dbh->err != 0) { 
	($dbh->err != 3807) ? die $dbh->errstr : warn $dbh->errstr . "\n" ;
}

if (defined($dmsth)) {
#
#	only execute if macro already exists
#
	defined($dmsth->execute) || die ($dmsth->errstr . "\n");
	$dmsth->finish || die ($dmsth->errstr . "\n");
}

my $cmsth = $dbh->prepare( 
'CREATE MACRO dbitest(col1 integer, col2 smallint, col3 byteint, col4 char(20), '
. 'col5 varchar(100), col6 float, col7 decimal(2,1), '
. 'col8 decimal(4,2), col9 decimal(8,4), col10 decimal(14,5)) '
. 'AS ( INSERT INTO alltypetst VALUES(:col1, :col2, :col3, :col4, :col5, :col6, '
. ':col7, :col8, :col9, :col10, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP(0));'
. 'SELECT * FROM alltypetst; );' ) || die ($dbh->errstr . "\n");
defined($cmsth->execute) || die ($cmsth->errstr . "\n");
$cmsth->finish || die ($cmsth->errstr . "\n");
warn "DROP/CREATE MACRO ok.\n";
#
#	now test all datatypes as bound params
#	and placeholders
#
warn "Testing multiple prepared statements, placeholders, and explicit commit...\n";
$dbh->STORE('AutoCommit', 0);

my $isth = $dbh->prepare( 
'INSERT INTO alltypetst VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
. 'CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP(0))') || die ($dbh->errstr . "\n");
my $dsth = $dbh->prepare( 'DELETE FROM alltypetst') || die ($dbh->errstr . "\n");
$ssth = $dbh->prepare('SELECT * FROM alltypetst ORDER BY col1') || die ($dbh->errstr . "\n");
#
#	insert a row
#
$isth->bind_param(1, 123456) || die ($isth->errstr . "\n");
$isth->bind_param(2, 1234) || die ($isth->errstr . "\n");
$isth->bind_param(3, 12) || die ($isth->errstr . "\n");
$isth->bind_param(4, 'deanie weenie') || die ($isth->errstr . "\n");
$isth->bind_param(5, 'okey dokey') || die ($isth->errstr . "\n");
$isth->bind_param(6, 12.34567) || die ($isth->errstr . "\n");
$isth->bind_param(7, 1.2) || die ($isth->errstr . "\n");
$isth->bind_param(8, 12.34) || die ($isth->errstr . "\n");
$isth->bind_param(9, 1234.5678) || die ($isth->errstr . "\n");
$isth->bind_param(10, 123456789.01234) || die ($isth->errstr . "\n");

defined($isth->execute) || die ($isth->errstr . "\n");
$isth->finish || die ($isth->errstr . "\n");
#
#	make sure the returned values are the same
#	as we inserted
#
$names = $ssth->{NAME};
defined($ssth->execute) || die ($ssth->errstr . "\n");
while (@row = $ssth->fetchrow_array() ) {
	for (my $field = 0; $field < scalar(@row); $field++) {
		if (defined($row[$field])) {
			print "$$names[$field]: $row[$field]\n";
		}
		else {
			print "$$names[$field]: NULL\n";
		}
	}
	print "\n";
}
$ssth->finish || die ($ssth->errstr . "\n");
#
#	clean up
#
$dbh->commit || die ($dbh->errstr . "\n");

$dbh->STORE('AutoCommit', 1);

defined($dsth->execute) || die ($dsth->errstr . "\n");
$dsth->finish || die ($dsth->errstr . "\n");

warn "DELETE/Parameterized INSERT/SELECT, and commit() ok.\n";
#
#	test the MACRO execution
#
warn "Testing MACRO execution w/ placeholders...\n";
$isth = $dbh->prepare( 
'USING (col1 INTEGER, col2 SMALLINT, col3 BYTEINT, col4 char(20), '
. 'col5 varchar(100), col6 float, col7 decimal(2,1), '
. 'col8 decimal(4,2), col9 decimal(8,4), col10 decimal(14,5)) '
. 'EXEC dbitest(:col1, :col2, :col3, :col4, :col5, :col6, :col7, :col8, :col9, :col10)') || die ($dbh->errstr . "\n");
$isth->bind_param(1, 123456) || die ($isth->errstr . "\n");
$isth->bind_param(2, 1234) || die ($isth->errstr . "\n");
$isth->bind_param(3, 12) || die ($isth->errstr . "\n");
$isth->bind_param(4, 'deanie weenie') || die ($isth->errstr . "\n");
$isth->bind_param(5, 'okey dokey') || die ($isth->errstr . "\n");
$isth->bind_param(6, 12.34567) || die ($isth->errstr . "\n");
$isth->bind_param(7, 1.2) || die ($isth->errstr . "\n");
$isth->bind_param(8, 12.34) || die ($isth->errstr . "\n");
$isth->bind_param(9, 1234.5678) || die ($isth->errstr . "\n");
$isth->bind_param(10, 123456789.01234) || die ($isth->errstr . "\n");
defined($isth->execute) || die ($isth->errstr . "\n");

$stmtnum = $isth->{'tdat_stmt_num'};
for (my $i = 1; $i <= $stmtnum; $i++) {
	$stmtinfo = $isth->{'tdat_stmt_info'};
	$stmthash = $$stmtinfo[$i];
	print "\nStatement $i:\n";
	foreach $stmtattr (keys(%$stmthash)) {
		if (defined($$stmthash{$stmtattr})) {
			print "$stmtattr is $$stmthash{$stmtattr}\n";
		}
		else {
			print "$stmtattr is undefined\n";
		}
	}
}

while (@row = $isth->fetchrow_array() ) {
	for (my $field = 0; $field < scalar(@row); $field++) {
		if (defined($row[$field])) {
			print "$$names[$field]: $row[$field]\n";
		}
		else {
			print "$$names[$field]: NULL\n";
		}
	}
	print "\n";
}
$isth->finish || die ($isth->errstr . "\n");
warn "MACRO execution ok.\n";
#
#	test summary support
#
warn "Testing summarized SELECT...\n";
my $sumsth = $dbh->prepare(
'select col1, col2, col9 from alltypetst with avg(col2), avg(col9) by col1 with sum(col2)') || die ($dbh->errstr . "\n");
$names = $sumsth->{NAME};
defined($sumsth->execute) || die ($ssth->errstr . "\n");
my $stmtnum = $sumsth->{'tdat_stmt_num'};
my $stmtinfo = $sumsth->{'tdat_stmt_info'};
my $stmthash = $$stmtinfo[1];
foreach $stmtattr (keys(%$stmthash)) {
	if (defined($$stmthash{$stmtattr})) {
		print "$stmtattr is $$stmthash{$stmtattr}\n";
	}
	else {
		print "$stmtattr is undefined\n";
	}
}
my $sumstarts = $$stmthash{'SummaryStarts'};
my $sumends = $$stmthash{'SummaryEnds'};
my $colstart = $$stmthash{'StartsAt'};
my $colend = $$stmthash{'EndsAt'};

my @row;
while (@row = $sumsth->fetchrow_array() ) {
	if (defined($$stmthash{'IsSummary'})) {
		my $issum = $$stmthash{'IsSummary'};
		print "\n-------------------------------------\n";
		my $sumpos = $$stmthash{'SummaryPosition'};
		my $sumposst = $$stmthash{'SummaryPosStart'};
		for (my $i = $$sumstarts[$issum], my $j = $$sumposst[$issum];
			$i <= $$sumends[$issum]; $i++, $j++) {
			print ("\t" x $$sumpos[$j]);
			print "$$names[$i] = $row[$i],\n";
		}
		print "\n";
	}
	else {
		for (my $i = $colstart; $i <= $colend; $i++) {
			print "$$names[$i] = $row[$i], ";
		}
		print "\n";
	}
}
$sumsth->finish || die ($sumsth->errstr . "\n");
warn "Summarized SELECT ok.\n";
#
#	large response check
#
if (1 == 2) {
warn "Testing large response (>32K)...\n";
$sth = $dbh->prepare( 'SELECT * FROM dbc.columns') || die ($dbh->errstr . "\n");
defined($sth->execute) || die ($sth->errstr . "\n");
$stmtinfo = $sth->{'tdat_stmt_info'};
$stmthash = $$stmtinfo[1];
foreach $stmtattr (keys(%$stmthash)) {
	if (defined($$stmthash{$stmtattr})) {
		print "$stmtattr is $$stmthash{$stmtattr}\n";
	}
	else {
		print "$stmtattr is undefined\n";
	}
}
$names = $sth->{NAME};
my $rowcnt = 0;
while (my @row = $sth->fetchrow_array() ) {
	$rowcnt++;
	if ($rowcnt%200 != 0) { next; }
	print "Row $rowcnt:\n";
	for (my $i = 0; $i < scalar(@$names); $i++) {
		if (defined($row[$i])) {
			print "$$names[$i]: $row[$i]\n";
		}
		else {
			print "$$names[$i]: NULL\n";
		}
	}
}
$sth->finish || die ($sth->errstr . "\n");
warn "Large response ok.\n";
}
#
#	test multisession nonblock mode with raw input
#
warn "Testing non-blocking multisession with raw input...\n";
my @dbhs;
my @sths;
my @inserts;
my @states;
my ($i, $j);
my $sescnt = 6;
for ($i = 0; $i < $sescnt; $i++) {
	$dbhs[$i] = DBI->connect("dbi:Teradata:$dsn", $userid, $passwd,
		{
			PrintError => 0,
			RaiseError => 0,
			AutoCommit => 0
		}
	) || die "Can't connect to $dsn: $DBI::errstr. Exitting...\n";
}

$dbhs[0]->do('DELETE FROM alltypetst'); 
$dbhs[0]->commit;
if ($dbhs[0]->err != 0) {
	die ($dbhs[0]->errstr . "\n");
}
for ($i = 0; $i < $sescnt; $i++) {
	$sths[$i] = $dbhs[$i]->prepare(
		'USING (col1 integer, col2 smallint, col3 byteint, col4 char(20), ' .
		'col5 varchar(100), col6 float, col7 decimal(2,1), ' .
		'col8 decimal(4,2), col9 decimal(8,4), col10 FLOAT)' .
		'INSERT INTO alltypetst VALUES(:col1, :col2, :col3, :col4, :col5, ' .
		':col6, :col7, :col8, :col9, :col10, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP(0))',
		{
			tdat_raw => IndicatorMode,
			tdat_nowait => 1
		}) || die ($dbhs[$i]->errstr . "\n");
	$data = rawinput($i);
	$sths[$i]->bind_param(1, $data) || die ($sths[$i]->errstr . "\n");
	defined($sths[$i]->execute)  || die ($sths[$i]->errstr . "\n");
	$states[$i] = 1;
	$inserts[$i] = 1;
}

my @params = (\@dbhs, -1);
for ($i = $sescnt; $i < 10000; $i++) {
	$j = $drh->func(@params, FirstAvailable);
	if ($j < 0) {
		print " While loading data: " . $drh->errstr . "\n";
		last;
	}
	$rows = $sths[$j]->func(undef, Realize);
	if (!defined($rows)) {
		die ($sths[$j]->errstr . "\n");
	}
	if ($inserts[$j]%25 == 0) {
		$dbhs[$j]->commit || die ($dbhs[$j]->errstr . "\n");
	}
	$data = rawinput($i);
	$sths[$j]->bind_param(1, $data) || die ($sths[$j]->errstr . "\n");
	defined($sths[$j]->execute) || die ($sths[$j]->errstr . "\n");
	$inserts[$j]++;
	if ($i%100 == 0) { print "Inserting row $i\n"; }
}

for ($i = 0; $i < $sescnt; $i++) {
	$j = $drh->func(@params, FirstAvailable);
	if ($j < 0) {
		print " While wrapping up: " . $drh->errstr . "\n";
		last;
	}
	$rows = $sths[$j]->func(undef, Realize);
	if (!defined($rows)) {
		die ($sths[$j]->errstr . "\n");
	}
}
for ($i = 0; $i < $sescnt; $i++) {
	$dbhs[$i]->commit || die ($dbhs[$i]->errstr . "\n");
	$sths[$i]->finish;
	$dbhs[$i]->disconnect;
}
warn "Non-blocking multisession w/ raw input ok.\n";
#
#	test raw output mode
#
warn "Testing raw output mode...\n";
$ssth = $dbh->prepare('SELECT * FROM alltypetst', {
	'tdat_raw' => 'IndicatorMode'
	}) || die ($dbh->errstr . "\n");
$names = $ssth->{NAME};
foreach $name (@$names) { print "$name "; }
print "\n";

open(OUTF, ">tdrawtest.out");
binmode OUTF;
defined($ssth->execute) || die ($ssth->errstr . "\n");
$reccnt = 0;
while (@row = $ssth->fetchrow_array() ) {
	my $rowlen = length($row[0]);
	$reccnt++;
	if ($reccnt%100 == 0) {
		print STDERR "Got $reccnt rows\n";
	}
	print OUTF $row[0];
}
close(OUTF);
$ssth->finish;
warn "Raw output ok.\n";
warn "Cleaning up...\n";
$dbh->do('DROP TABLE alltypetst');
$dbh->do('DROP MACRO dbitst');
warn "Logging off...\n";
$dbh->disconnect();
warn "Tests completed ok, exitting...\n";

sub rawinput {
#
# col1 integer
# col2 smallint
# col3 byteint
# col4 char(20)
# col5 varchar(100)
# col6 float
# col7 decimal(2,1)
# col8 decimal(4,2)
# col9 decimal(8,4)
# col10 decimal(14,5), but send a float
#
	local ($inp) = @_;
	local $col5;
	if ($inp%20 == 0) { $indic = 16; $col5 = '';}
	else { $indic = 0;  $col5 = rndstring(int(rand(99))+1); }
	local $col6 = rand(100000);
	local $col7 = int(rand(99));
	local $col8 = int(rand(999));
	local $col9 = int(rand(99999));
	local $col10a = int(rand(99999999));
	local $col10b = int(rand(99999));
	local $col10 = $col10a . '.' . $col10b;
	local $len = 2 + 4 + 2 + 1 + 20 + 2 + length($col5) + 8 + 1 + 2 + 4 + 8;
	$rec = pack("Scc l s c A20 SA* d C S L d c", 
		$len, $indic, 0, 
		$inp, $inp%32767, $inp%255, rndstring(20),
		length($col5), $col5, $col6, $col7, $col8, $col9, $col10, 10);
	return $rec;
}

sub rndstring {
	local($len) = pop(@_);
	local($s) = '';
	for (my $j = 0; $j < $len; $j++) {
		$s .= substr($alphas, rand(length($alphas)), 1);
	}
	return $s;
}

