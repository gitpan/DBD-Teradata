use DBI;
use DBI qw{:sql_types};
use DBD::Teradata;
#
#	Modify DBD::Teradata as follows:
#
#		any prepare on MONITOR connection allocs
#		an empty param array with empty type and length
#		arrays, and sets NUM_OF_PARAMS to 16
#
#		the application *must* supply datatype info for
#		each bound parameter; otherwise, default
#		binding info is used to construct the request
#		sent to the DBMS, which is likely to result
#		in incorrect results.
#
#		on execute, any DATAINFO parcel recvd will be processed
#		into a set of column binding info, and NUM_OF_FIELDS
#		will be set to the number of fields reported. Note this
#		will cause field data types, precisions, etc. to change
#		between fetches!
#
#		the application should avoid explicitly binding columns,
#		since the number/type of columns returned may change between
#		fetches.
#
#	catalog of PMPC commands and their returned data fields
#
@monnames1 = ('SampleSec');

@monnames2 = (
'HostId',
'LogonPENo',
'RunVProcNo',
'SessionNo',
'UserName[30]',
'UserAccount[30]',
'UserId',
'LSN',
'LogonTime',
'LogonDate',
'PartName[16]',
'Priority[2]',
'PEState[18]',
'PECPUSec',
'XactCount',
'ReqCount',
'ReqCacheHits',
'AMPState[18]',
'AMPCPUSec',
'AMPIO',
'Delta_AMPSpool',
'Blk_1_HostId',     
'Blk_1_SessNo',     
'Blk_1_UserID',     
'Blk_1_LMode',      
'Blk_1_OType',      
'Blk_1_ObjDBId',    
'Blk_1_ObjTId',     
'Blk_1_Status',     
'Blk_2_HostId',     
'Blk_2_SessNo',     
'Blk_2_UserID',     
'Blk_2_LMode',      
'Blk_2_OType',      
'Blk_2_ObjDBId',    
'Blk_2_ObjTId',     
'Blk_2_Status',     
'Blk_3_HostId',     
'Blk_3_SessNo',     
'Blk_3_UserID',     
'Blk_3_LMode',      
'Blk_3_OType',      
'Blk_3_ObjDBId',    
'Blk_3_ObjTId',     
'Blk_3_Status',     
'MoreBlockers',     
'LogonSource_len',  
'LogonSource'
);

# DBI->trace(2, 'dbitrace.log');

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

	my $ctldbh = DBI->connect("dbi:Teradata:dbc", 'dbc', 'dbc',
		{
			PrintError => 0,
			RaiseError => 0,
			AutoCommit => 0,
			tdat_utility => 'MONITOR'
		}
	);
	if (!defined($ctldbh)) {
		print ($DBI::errstr . "\n");
		exit;
	}
	my $drh = $ctldbh->{'Driver'};
	$rate = 30;
#
#	need to set session rate first
#
	$ratesth = $ctldbh->prepare('SET SESSION RATE');
	if (!defined($ratesth)) {
		print ($ctldbh->errstr . "\n");
		$ctldbh->disconnect;
		exit;
	}
	
	$ratesth->bind_param(1, 2, SQL_SMALLINT);	# version id
	$ratesth->bind_param(2, $rate, SQL_SMALLINT);	# rate
	$ratesth->bind_param(3, undef, 				# system wide
		{ TYPE => SQL_CHAR, PRECISION => 1 });

	print "Setting session rate to $rate...\n";
	$rc = $ratesth->execute;
	if (!defined($rc)) {
		print ($ratesth->errstr . "\n");
		$ctldbh->disconnect;
		exit;
	}
#
#	check our column descriptions, and bind accordingly
#
	my ($types, $precs, $scales, $stmt);
	$stmt = 0;
	while (@row = $ratesth->fetchrow_array) {
		if ($stmt != $ratesth->{'tdat_stmt_num'}) {
			$stmt = $ratesth->{'tdat_stmt_num'};
			$types = $ratesth->{TYPE};
			$precs = $ratesth->{PRECISION};
			$scales = $ratesth->{SCALE};
			print "Statement $stmt returns\n";
			for ($i = 0; $i < scalar(@$types); $i++) {
				if ($$types[$i] == SQL_DECIMAL) {
					print "\tcol$i: DECIMAL($$precs[$i], $$scales[$i])\n"; 
				}
				elsif (($$types[$i] == SQL_CHAR) || ($$types[$i] == SQL_VARCHAR)) {
					print "\tcol$i: $typestr{$$types[$i]}($$precs[$i])\n"; 
				}
				else {
					print "\tcol$i: $typestr{$$types[$i]}\n"; 
				}
			}
			print "\n";
		}
		foreach $val (@row) {
			if (!defined($val)) {
				print "NULL, ";
			}
			else {
				print "$val, ";
			}
		}
	}
#
#	then wait a while for data to be collected
#
	print "Waiting for sample...\n";
	sleep ($rate+2);
#
#	now see what we've got
#
	$sth = $ctldbh->prepare('MONITOR SESSION');
	if (!defined($sth)) {
		print ($ctldbh->errstr . "\n");
		$ctldbh->disconnect;
		exit;
	}
	
	$sth->bind_param(1, 2, SQL_SMALLINT);	# version id
	$sth->bind_param(2, undef, SQL_SMALLINT);	# host id
	$sth->bind_param(3, undef, 			# username
		{ TYPE => SQL_CHAR, PRECISION => 30 });
	$sth->bind_param(4, undef, SQL_INTEGER);	# session no

	$rc = $sth->execute;
	if (!defined($rc)) {
		print ($sth->errstr . "\n");
		$ctldbh->disconnect;
		exit;
	}
	print "Fetching MONITOR SESSION results...\n";
#
#	check our column descriptions, and format accordingly
#
	$stmt = 0;
	while (@row = $sth->fetchrow_array) {
		if ($stmt != $sth->{'tdat_stmt_num'}) {
			$stmtinfo = $sth->{'tdat_stmt_info'};
			$stmt = $sth->{'tdat_stmt_num'};
			$types = $sth->{TYPE};
			$precs = $sth->{PRECISION};
			$scales = $sth->{SCALE};
			$stmthash = $$stmtinfo[$stmt];
			$fldcnt = $$stmthash{'EndsAt'};
			print "Statement $stmt returns\n";
			for ($i = 0; $i <= $fldcnt; $i++) {
				if ($$types[$i] == SQL_DECIMAL) {
					print "\tcol$i: DECIMAL($$precs[$i], $$scales[$i])\n"; 
				}
				elsif (($$types[$i] == SQL_CHAR) || ($$types[$i] == SQL_VARCHAR)) {
					print "\tcol$i: $typestr{$$types[$i]}($$precs[$i])\n"; 
				}
				else {
					print "\tcol$i: $typestr{$$types[$i]}\n"; 
				}
			}
			print "\n";
		}
		foreach $val (@row[0..$fldcnt]) {
			if (!defined($val)) {
				print "NULL, ";
			}
			else {
				print "$val, ";
			}
		}
	}
	$sth->finish;
#
#	clear session rate when done
#
	print "\nTurning off session sampling...\n";

	$ratesth->bind_param(1, 2, SQL_SMALLINT);	# version id
	$ratesth->bind_param(2, 0, SQL_SMALLINT);	# rate
	$ratesth->bind_param(3, undef, 			# system wide
		{ TYPE => SQL_CHAR, PRECISION => 1 });

	$rc = $ratesth->execute;
	if (!defined($rc)) {
		print ($ratesth->errstr . "\n");
		$ctldbh->disconnect;
		exit;
	}

	$stmt = 0;
	while (@row = $ratesth->fetchrow_array) {
		if ($stmt != $ratesth->{'tdat_stmt_num'}) {
			$stmt = $ratesth->{'tdat_stmt_num'};
			$types = $ratesth->{TYPE};
			$precs = $ratesth->{PRECISION};
			$scales = $ratesth->{SCALE};
			print "Statement $stmt returns\n";
			for ($i = 0; $i < scalar(@$types); $i++) {
				if ($$types[$i] == SQL_DECIMAL) {
					print "\tcol$i: DECIMAL($$precs[$i], $$scales[$i])\n"; 
				}
				elsif (($$types[$i] == SQL_CHAR) || ($$types[$i] == SQL_VARCHAR)) {
					print "\tcol$i: $typestr{$$types[$i]}($$precs[$i])\n"; 
				}
				else {
					print "\tcol$i: $typestr{$$types[$i]}\n"; 
				}
			}
			print "\n";
		}
		foreach $val (@row) {
			if (!defined($val)) {
				print "NULL, ";
			}
			else {
				print "$val, ";
			}
		}
	}
	$ratesth->finish;
	$ctldbh->disconnect;
	