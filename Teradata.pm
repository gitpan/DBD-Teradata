#
#	Copyright (c) 2000, Dean Arnold, USA<br>
#	Copyright (c) 2001-2004, Presicient Corp., USA
#
# Permission is granted to use this software according to the terms of the
# Artistic License, as specified in the Perl README file,
# with the exception that commercial redistribution, either 
# electronic or via physical media, as either a standalone package, 
# or incorporated into a third party product, requires prior 
# written approval of the author.
#	
#	This program is distributed in the hope that it will be useful,
#	but WITHOUT ANY WARRANTY; without even the implied warranty of
#	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#	
#	Presicient Corp. reserves the right to provide support for this software
#	to individual sites under a separate (possibly fee-based)
#	agreement.
#
#
require 5.006;

package DBD::Teradata;

use Exporter;
use DBI qw(:sql_types);

use vars qw($VERSION $err $errstr $state $drh %connections);

use strict;

BEGIN {
our @ISA = qw(Exporter);
our @EXPORT    = ();
our @EXPORT_OK = qw(
	$phdfltsz %ppackstr %ptypecodes %ptypeszs %ptypemap);
};

our $phdfltsz = 16;
our %ppackstr = (
	SQL_VARCHAR, 'Sa', 
	SQL_CHAR, 'A', 
	SQL_FLOAT, 'd', 
	SQL_DECIMAL, 'i2',
	SQL_INTEGER, 'i', 
	SQL_SMALLINT, 's', 
	SQL_TINYINT, 'c', 
	SQL_VARBINARY, 'Sa',
	SQL_BINARY, 'a',
	SQL_LONGVARBINARY, 'Sa',
	SQL_DATE, 'a8',
	SQL_TIMESTAMP, 'a20',
	SQL_TIME, 'a8'
	);
our %ptypecodes = ( 
	SQL_VARCHAR, 448, 
	SQL_CHAR, 452, 
	SQL_FLOAT, 480, 
	SQL_DECIMAL, 484,
	SQL_INTEGER, 496, 
	SQL_SMALLINT, 500, 
	SQL_TINYINT, 756, 
	SQL_VARBINARY, 688,
	SQL_BINARY, 692,
	SQL_LONGVARBINARY, 696,
	SQL_DATE, 752,
	SQL_TIMESTAMP, 760,
	SQL_TIME, 764
	);

our %ptypeszs = ( 
	SQL_VARCHAR, 32000, 
	SQL_CHAR, 32000, 
	SQL_FLOAT, 8, 
	SQL_DECIMAL, 8,
	SQL_INTEGER, 4, 
	SQL_SMALLINT, 2, 
	SQL_TINYINT, 1, 
	SQL_VARBINARY, 32000,
	SQL_BINARY, 32000,
	SQL_LONGVARBINARY, 32000,
	SQL_DATE, 8,
	SQL_TIMESTAMP, 20,
	SQL_TIME, 8
	);

our %ptypemap = ( 
	448, SQL_VARCHAR,
	452, SQL_CHAR,
	480, SQL_FLOAT,
	484, SQL_DECIMAL,
	496, SQL_INTEGER,
	500, SQL_SMALLINT, 
	756, SQL_TINYINT,
	688, SQL_VARBINARY,
	692, SQL_BINARY,
	696, SQL_LONGVARBINARY,
	752, SQL_DATE,
	760, SQL_TIMESTAMP,
	764, SQL_TIME
	);

our $VERSION = "1.20";
our $drh = undef;
our %connections = ();
our $err = 0;
our $errstr = '';
our $state = '00000';

sub driver {
	return $drh if $drh;
	my($class, $attr) = @_;
	$class .= '::dr';
	
	$drh = DBI::_new_drh($class,
		{
			'Name' => 'Teradata',
			'Version' => $VERSION,
			'Err' => \$DBD::Teradata::err,
			'Errstr' => \$DBD::Teradata::errstr,
			'State' => \$DBD::Teradata::state,
			'Attribution' => 'DBD::Teradata by D. Arnold'
		});
	DBI->trace_msg("DBD::Teradata v.$VERSION loaded on $^O\n", 1);
	DBD::Teradata::impl::init();
	return $drh;
}

1;

package DBD::Teradata::dr;

use strict;

our $imp_data_size = 0;

sub connect {
	my ($drh, $dsn, $user, $auth, $attr) = @_;
	my $host;
	my $port;
	if ($dsn =~ /^(.*):(\d+)$/) {
		($host,$port) = ($1,$2);
	}
	elsif ($dsn =~ /^(.*)$/) {
		$host = $1;
		$port = 1025;
	}
	else {
		$drh->DBI::set_err('08001', "Malformed dsn $dsn");
		return undef;
	}
	my $sessno = DBD::Teradata::impl::connect($host, $port, $user, $auth, undef, 
		\$DBD::Teradata::err, \$DBD::Teradata::errstr);
	if (!defined($sessno)) {
		return undef;
	}
	my $dbh = DBI::_new_dbh($drh,{
		'Name' => $dsn,
		'USER' => $user,
		'CURRENT_USER' => $user
	});
	$dbh->STORE('tdat_host', $host);
	$dbh->STORE('tdat_sessno', $sessno);
	$DBD::Teradata::connections{$sessno} = $dbh;
	$DBD::Teradata::impl::sesinxact{$sessno} = 0;
	$DBD::Teradata::err = undef;
	$DBD::Teradata::errstr = undef;
	$dbh;
}
sub data_sources {}

sub DESTROY {
	disconnect_all();
}

sub disconnect_all {
	my $dbh;
	my $sessno;
	foreach $sessno (keys(%DBD::Teradata::connections)) {
		$dbh = $DBD::Teradata::connections{$sessno};
		if (defined($dbh)) {
			$dbh->disconnect();
		}
	}
	reset %DBD::Teradata::connections;
}
sub FirstAvailable {
	my($drh, $dbhlist, $timeout) = @_;
	my $i = 0;
	my @sesslist;
	if (!defined($timeout)) { $timeout = -1; }
	my $dbh;
	foreach $dbh (@$dbhlist) {
		if (!defined($dbh)) { next; }
		$sesslist[$i++] = $dbh->{'tdat_sessno'};
	}
	my $sessno = DBD::Teradata::impl::FirstAvailable(\@sesslist, $timeout);
	if (!defined($sessno)) { return undef; }
	for ($i = 0; $i < scalar(@$dbhlist); $i++) {
		if ((defined($$dbhlist[$i])) &&
			($sessno == $$dbhlist[$i]->{'tdat_sessno'})) {
			return $i;
		}
	}
	return undef;
}
sub FirstAvailList {
	my($drh, $dbhlist, $timeout) = @_;
	my $i = 0;
	my @sesslist;
	if (!defined($timeout)) { $timeout = -1; }
	my $dbh;
	foreach $dbh (@$dbhlist) {
		if (!defined($dbh)) { next; }
		$sesslist[$i++] = $dbh->{'tdat_sessno'};
	}
	my @outlist = DBD::Teradata::impl::FirstAvailList(\@sesslist, $timeout);
	if (!@outlist) { return undef; }
	my @outdbhs = ();
	foreach $i (@outlist) {
		for (my $j = 0; $j < scalar(@$dbhlist); $j++) {
			if ((defined($$dbhlist[$j])) && ($i == $$dbhlist[$j]->{'tdat_sessno'})) { 
				push(@outdbhs, $j);
				last;
			}
		}
	}
	return @outdbhs;
}

1;

package DBD::Teradata::db;

use strict;

our $imp_data_size = 0;

sub prepare {
	my($dbh, $stmt, $attribs) = @_;
	
	my $attr;
	if (defined($attribs)) {
		foreach $attr (keys(%$attribs)) {
			if (($attr ne 'tdat_nowait') && ($attr ne 'tdat_raw') && 
				($attr ne 'tdat_keepresp') && ($attr ne 'tdat_clone')) {
				$DBD::Teradata::err = -1;
				$DBD::Teradata::errstr = "Unknown statement attribute \"$attr\".";
				return undef;
			}
			elsif (($attr eq 'tdat_raw') && ($$attribs{$attr} ne 'RecordMode') &&
				($$attribs{$attr} ne 'IndicatorMode')) {
				$DBD::Teradata::err = -1;
				$DBD::Teradata::errstr = 'Invalid raw mode value.';
				return undef;
			}
			elsif ($attr eq 'tdat_clone') {
				my $csth = $$attribs{'tdat_clone'};
				if (ref $csth ne 'DBI::st') {
					$DBD::Teradata::err = -1;
					$DBD::Teradata::errstr = 'tdat_clone value must be DBI statement handle.';
					return undef;
				}
			}
		}
	}

	my $sessno = $dbh->{'tdat_sessno'};
	$stmt =~s/[\n\r\t]/ /g;
	my @fname = ();
	my @fname_uc = ();
	my @fname_lc = ();
	my @ftype = ();
	my @ftitle = ();
	my @fformat = ();
	my @fprec = ();
	my @fscale = ();
	my @fnullable = ();
	my @ptypes = ();
	my @plens = ();
	my $usephs = 0;
	my @stmtinfo = ();

	my @acttype = ();
	my @actcount = ();
	my @actwarns = ();
	my @actstarts = ();
	my @actends = ();
	my @actsumstarts = ();
	my @actsumends = ();
	my $issum = undef;
	my $numparams = 0;
	
		$numparams = DBD::Teradata::impl::prepare($sessno, $stmt, 
			\@fname, \@ftype, \@fprec, \@fscale, \@fnullable, \@ptypes, \@plens,
			\$usephs, \@acttype, \@actcount, \@actwarns,
			\@actstarts, \@actends, \@actsumstarts, \@actsumends, \@ftitle, \@fformat);
		if (!defined($numparams)) {
			$DBD::Teradata::err = DBD::Teradata::impl::err($sessno);
			$DBD::Teradata::errstr = DBD::Teradata::impl::errstr($sessno);
			return undef;
		}

		for (my $i = 1; $i <= scalar(@acttype); $i++) {
			my %stmthash;
			$stmthash{'ActivityType'} = (defined($acttype[$i])) ? $acttype[$i] : undef;
			$stmthash{'ActivityCount'} = (defined($actcount[$i])) ? $actcount[$i] : undef;
			$stmthash{'Warning'} = (defined($actwarns[$i])) ? $actwarns[$i] : undef;
			$stmthash{'StartsAt'} = (defined($actstarts[$i])) ? $actstarts[$i] : undef;
			$stmthash{'EndsAt'} = (defined($actends[$i])) ? $actends[$i] : undef;
			$stmthash{'IsSummary'} = undef;
			$stmthash{'SummaryStarts'} = (defined($actsumstarts[$i])) ? 
				$actsumstarts[$i] : undef;
			$stmthash{'SummaryEnds'} = (defined($actsumends[$i])) ? 
				$actsumends[$i] : undef;
			$stmtinfo[$i] = \%stmthash;
		}
	my ($outer, $sth) = DBI::_new_sth($dbh, { Statement => $stmt });
	$sth->STORE('tdat_nowait', 0);
	if (defined($attribs)) {
		foreach $attr (keys(%$attribs)) {
			if ($attr eq 'tdat_clone') { next; }
			$sth->STORE($attr, $$attribs{$attr});
		}
	}
	$sth->STORE('tdat_sessno', $sessno);
	$sth->STORE('tdat_stmt_num' => 0);
	$sth->STORE('tdat_stmt_info' => \@stmtinfo);
	$sth->STORE('tdat_rows' => -1);
	my @params = ();
	$sth->STORE('tdat_params' => \@params);
	$sth->STORE('tdat_ptypes' => \@ptypes);
	$sth->STORE('tdat_plens' => \@plens);
	$sth->STORE('tdat_usephs' => $usephs);
	$sth->STORE('NUM_OF_PARAMS' => $numparams);

	$sth->STORE('NUM_OF_FIELDS' => scalar(@fname));
	for (my $i = 0; $i < scalar(@fname); $i++) {
		$fname_lc[$i] = "\L$fname[$i]\E";
		$fname_uc[$i] = "\U$fname[$i]\E";
	}
	
	$sth->{NAME} = \@fname;
	$sth->{NAME_lc} = \@fname_lc;
	$sth->{NAME_uc} = \@fname_uc;
	$sth->{TYPE} = \@ftype;
	$sth->{PRECISION} = \@fprec;
	$sth->{SCALE} = \@fscale;
	$sth->{NULLABLE} = \@fnullable;
	$sth->{tdat_TITLE} = \@ftitle;
	$sth->{tdat_FORMAT} = \@fformat;
	$outer;
}
		
sub DESTROY {
	my $dbh = shift;
	$dbh->disconnect();
}

sub disconnect {
	my $dbh = shift;
	my $i;
	my $sessno  = $dbh->{'tdat_sessno'};
	if ((!defined($sessno)) ||
		(!defined($DBD::Teradata::connections{$sessno}))) {
		return undef; 
	}
	undef $DBD::Teradata::connections{$sessno};
	DBD::Teradata::impl::disconnect($sessno);
	1;
}

sub commit {
	my ($dbh) = @_;
	my $xactmode = $dbh->FETCH('AutoCommit');
	
	if ($xactmode == 1) {
		if ($dbh->FETCH('Warn')) {
			warn("Commit ineffective while AutoCommit is on");
		}
		return 1;
	}
	my $sessno = $dbh->FETCH('tdat_sessno');

	if ($DBD::Teradata::impl::sesinxact{$sessno} != 0) {
		DBD::Teradata::impl::tddo($sessno, 'ET;');
		$DBD::Teradata::impl::sesinxact{$sessno} = 0;
	}
	1;
}

sub rollback {
	my ($dbh) = @_;
	my $xactmode = $dbh->FETCH('AutoCommit');
	
	if ($xactmode == 1) {
		if ($dbh->FETCH('Warn')) {
			warn("Rollback ineffective while AutoCommit is on");
		}
		return 1;
	}
	my $sessno = $dbh->FETCH('tdat_sessno');
	if ($DBD::Teradata::impl::sesinxact{$sessno} != 0) {
		DBD::Teradata::impl::tddo($sessno, 'ABORT;');
		$DBD::Teradata::impl::sesinxact{$sessno} = 0;
	}
	1;
}

sub STORE {
	my ($dbh, $attr, $val) = @_;
	if ($attr eq 'AutoCommit') {
		$dbh->{$attr} = $val;
		return 1;
	}
	if ($attr =~ /^tdat_/) {
		$dbh->{$attr} = $val;
		return 1;
	}
	$dbh->SUPER::STORE($attr, $val);
}

sub FETCH {
	my($dbh, $attr) = @_;
	if ($attr eq 'AutoCommit') {
		return $dbh->{$attr};
	}
	if ($attr =~ /^tdat_/) {
		return $dbh->{$attr};
	}
	$dbh->SUPER::FETCH($attr);
}

sub errstr {
	my($dbh) = @_;
	my $sessno = $dbh->{'tdat_sessno'};
	return DBD::Teradata::impl::errstr($sessno);
}

sub err {
	my($dbh) = @_;
	my $sessno = $dbh->{'tdat_sessno'};
	return DBD::Teradata::impl::err($sessno);
}
1;

package DBD::Teradata::st;

use strict;

our $imp_data_size = 0;

use DBI qw(:sql_types);
use DBD::Teradata qw($phdfltsz %ptypeszs %ptypecodes %ppackstr %ptypemap);

use constant tdat_NULL_MASK => 0xffe0;


sub BindColArray {
	my ($sth, $pNum, $ary, $maxlen) = @_;

	if (ref $ary ne 'ARRAY') {
		$DBD::Teradata::err = -1;
		$DBD::Teradata::errstr = 'BindColArray() requires arrayref parameter.';
		return undef;
	}
	if ($pNum <= 0) {
		$DBD::Teradata::err = -1;
		$DBD::Teradata::errstr = 'Invalid column number.';
		return undef;
	}
	
	my $c = $sth->FETCH('tdat_colary');
	if (!defined($c)) {
		my @colary = ();
		$c = \@colary;
		$sth->STORE('tdat_colary', \@colary);
	}
	$$c[$pNum] = $ary;
	if (defined($maxlen)) {
		my $ml = $sth->FETCH('tdat_maxcolary');
		if ((!defined($ml)) || ($ml < $maxlen)) {
			$sth->STORE('tdat_maxcolary', $maxlen);
		}
	}
	1;
}

sub bind_param {
	my ($sth, $pNum, $val, $attr) = @_;

	if (ref $val eq 'ARRAY') {
		$DBD::Teradata::err = -1;
		$DBD::Teradata::errstr = 'BindParamArray() not supported.';
		return undef;
	}
	my $type = SQL_VARCHAR;
	my $tlen = $phdfltsz;
	my $usephs = $sth->FETCH('tdat_usephs');
	if (($usephs) && (defined($attr))) {
		if (ref $attr) {
			$type = $attr->{TYPE};
			if (defined($attr->{PRECISION})) {
				$tlen = $attr->{PRECISION};
			}
			else {
				$tlen = $ptypeszs{$type};
			}
		}
		else {
			$type = $attr;
			$tlen = $ptypeszs{$type};
		}
	}

	my $params = $sth->FETCH('tdat_params');
	$$params[$pNum-1] = $val;
	if ($usephs) {
		my $ptypes = $sth->FETCH('tdat_ptypes');
		$ptypes->[$pNum-1] = $type;
		my $plens = $sth->FETCH('tdat_plens');
		if (($type == SQL_VARCHAR) ||
			($type == SQL_LONGVARCHAR) ||
			($type == SQL_LONGVARBINARY) ||
			($type == SQL_VARBINARY)) {
			$plens->[$pNum-1] = (defined($val)) ? 
				length($val) : $phdfltsz;
		}
		else {
			$plens->[$pNum-1] = $tlen;
		}
	}
	1;
}
*BindParamArray = \&bind_param;

sub bind_param_inout {
	my ($sth, $pNum, $val, $maxlen, $attr) = @_;
	return bind_param($sth, $pNum, $val, $attr);
}

sub execute {
	my ($sth, @bind_values) = @_;
	my $params = (@bind_values) ?
		\@bind_values : $sth->FETCH('tdat_params');

	my $numParam = $sth->FETCH('NUM_OF_PARAMS');
	if (!defined($numParam)) { $numParam = 0; }

	my $ptypes = $sth->FETCH('tdat_ptypes');
	my $plens = $sth->FETCH('tdat_plens');
	my $usephs = $sth->FETCH('tdat_usephs');

	my $sessno = $sth->FETCH('tdat_sessno');
	my $dbh = $DBD::Teradata::connections{$sessno};
	if ((@bind_values) && ($usephs != 0)) {
		for (my $i = 0; $i < $numParam; $i++) {
			$$ptypes[$i] = SQL_VARCHAR;
			$$plens[$i] = $phdfltsz;
		}
	}
	my $rawmode = $sth->FETCH('tdat_raw');
	if (($numParam != 0) && (defined($rawmode))) { $numParam = 1; }
	if (defined($params) && (@$params > $numParam)) {
		$DBD::Teradata::err = -1;
		$DBD::Teradata::errstr = 'Too many parameters provided.';
		return undef;
	}
	if ((!defined($params)) && ($numParam != 0)) {
		$DBD::Teradata::err = -1;
		$DBD::Teradata::errstr = 
			'No parameters provided for parameterized statement.';
		return undef;
	}
	my $stmtno = 0;
	my $maxparmlen = 1;
	for (my $i = 0; $i < $numParam; $i++) {
		if ((ref $$params[$i] eq 'ARRAY') &&
			(scalar(@{$$params[$i]}) > $maxparmlen)) { 
				$maxparmlen = scalar(@{$$params[$i]}); 
		}
	}

	my $datainfo = '';
	my $indicdata = '';
	my $fldcnt = $numParam;
	if (defined($params) && (@$params != 0)) {
		if ($usephs != 0) {
			my $ptypes = $sth->FETCH('tdat_ptypes');
			my $plens = $sth->FETCH('tdat_plens');
			my $i;
			for ($i = 0; $i < $fldcnt; $i++) {
				if ($$ptypes[$i] eq SQL_VARCHAR) {
					my $p = $$params[$i];
					if (defined($p)) {
						if (ref $p eq 'ARRAY') {
							$p = $$p[0];
						}
						elsif (ref $p eq 'SCALAR') {
							$p = $$p;
						}
					}
					if (defined($p) && (length($p) > $$plens[$i])) {
						$$plens[$i] = length($p);
					}
					$datainfo .= pack('SS', $ptypecodes{DBI::SQL_VARCHAR}+1, 
						2 + $$plens[$i]);
				}
				elsif ($$ptypes[$i] eq SQL_VARBINARY) {
					$datainfo .= pack('SS', $ptypecodes{DBI::SQL_VARBINARY}+1,
						2 + $$plens[$i]);
				}
				else {
					$datainfo .= pack('SS', $ptypecodes{$$ptypes[$i]}+1, 
						$$plens[$i]);
				}
			}
			$datainfo = pack('Sa*', $i, $datainfo);
		}
		for (my $k = 0; $k < $maxparmlen; $k++) {
			if (!defined($rawmode)) {
				my @indicvec = DBD::Teradata::impl::initIndic($fldcnt);
				my $ptypes = $sth->FETCH('tdat_ptypes');
				my $plens = $sth->FETCH('tdat_plens');
				for (my $i = 0; $i < $fldcnt; $i++) {
					my $p = $$params[$i];
					if (defined($p)) {
						if (ref $p eq 'ARRAY') {
							if (scalar(@$p) < $k) {
								undef $p;
							}
							else {
								$p = $$p[$k];
							}
						}
						elsif (ref $p eq 'SCALAR') {
							$p = $$p;
						}
					}
					if (!defined($p)) {
						DBD::Teradata::impl::setIndicator(\@indicvec, $i);
						if (($$ptypes[$i] eq SQL_VARCHAR) ||
							($$ptypes[$i] eq SQL_VARBINARY)) {
							$indicdata .= pack('S', 0);
						}
						elsif (($$ptypes[$i] eq SQL_CHAR) ||
							($$ptypes[$i] eq SQL_BINARY)) {
							$indicdata .= pack("A$$plens[$i]", '');
						}
						elsif ($$ptypes[$i] eq SQL_DECIMAL) {
							my $decsz = 8;
							my $prec = int($$plens[$i]/256);
							if ($prec <= 2) {
								$decsz = 1;
							}
							elsif ($prec <=  4) {
								$decsz = 2;
							}
							elsif ($prec <=  9) {
								$decsz = 4;
							}
							$indicdata .= pack("A$decsz", '');
						}
						else {
							$indicdata .= pack($ppackstr{$$ptypes[$i]}, 0);
						}
						next;
					}
					if (($$ptypes[$i] eq SQL_VARCHAR) ||
						($$ptypes[$i] eq SQL_VARBINARY)) {
						$indicdata .= pack('Sa*', length($p), $p);
					}
					elsif (($$ptypes[$i] eq SQL_CHAR) ||
						($$ptypes[$i] eq SQL_BINARY)) {
						$indicdata .= pack("A$$plens[$i]", $p);
					}
					elsif ($$ptypes[$i] eq SQL_DECIMAL) {
						$indicdata .= DBD::Teradata::impl::cvt_flt2dec($p, 
							int($$plens[$i]/256), int($$plens[$i]%256));
					}
					else {
						$indicdata .= pack($ppackstr{$$ptypes[$i]}, $p);
					}
				}
				$indicdata = DBD::Teradata::impl::cvtIndics(\@indicvec) . $indicdata;
				$rawmode = 'IndicatorMode';
			}
			else {
				my $p = $$params[0];
				if (defined($p)) {
					if (ref $p eq 'ARRAY') {
						if (scalar(@$p) < $k) {
							undef $p;
						}
						else {
							$p = $$p[$k];
						}
					}
					elsif (ref $p eq 'SCALAR') {
						$p = $$p;
					}
				}
				$indicdata = $p;
				$indicdata = substr($indicdata, 2, length($indicdata) - 3);
			}
		}
	}
	if ((!$dbh->FETCH('AutoCommit')) &&
		($DBD::Teradata::impl::sesinxact{$sessno} == 0)) {
		DBD::Teradata::impl::tddo($sessno, 'BT;');
		$DBD::Teradata::impl::sesinxact{$sessno} = 1;
	}
	
	my $rowcnt = DBD::Teradata::impl::execute( $sessno, 
		$sth->FETCH('Statement'), $datainfo, $indicdata, 
		$sth->FETCH('tdat_nowait'),
		$sth->FETCH('tdat_stmt_info'),
		\$stmtno, $rawmode, $sth->FETCH('tdat_keepresp'), $sth);
		
	$sth->STORE('tdat_stmt_num', $stmtno);
	$sth->STORE('tdat_rows', $rowcnt);
	if (!defined($rowcnt)) {
		$DBD::Teradata::err = DBD::Teradata::impl::err($sessno);
		$DBD::Teradata::errstr = DBD::Teradata::impl::errstr($sessno);
		return undef;
	}

	return (($rowcnt == 0) ? -1 : $rowcnt);
}

sub Realize {
	my ($sth) = @_;
	my $stmtno = 0;
	my $sessno = $sth->FETCH('tdat_sessno');
	my $rowcnt = DBD::Teradata::impl::Realize( $sessno, 
		$sth->FETCH('tdat_stmt_info'), \$stmtno);
	if (!defined($rowcnt)) {
		$DBD::Teradata::err = DBD::Teradata::impl::err($sessno);
		$DBD::Teradata::errstr = DBD::Teradata::impl::errstr($sessno);
	}
	$sth->STORE('tdat_stmt_num' => $stmtno);
	return $rowcnt;
}

sub fetch {
	my($sth) = @_;

	my $sessno = $sth->FETCH('tdat_sessno');
	my $stmtno = $sth->FETCH('tdat_stmt_num');
	my $nowait = $sth->FETCH('tdat_nowait');
	my $stmtinfo = $sth->FETCH('tdat_stmt_info');
	my $rawmode = $sth->FETCH('tdat_raw');
	my $colary = $sth->FETCH('tdat_colary');
	my $maxlen = $sth->FETCH('tdat_maxcolary');
	my $data = '';
	my @tmpary = ();
	my $ary = (defined($colary) ? (($rawmode) ? $$colary[1] : \@tmpary) : undef);

	my $rc = DBD::Teradata::impl::fetch($sessno,
		$nowait, $stmtinfo, \$stmtno, $ary, $maxlen, \$data, $sth);
	$sth->STORE('tdat_stmt_num' => $stmtno);
	if (!defined($rc)) { 
		$DBD::Teradata::err = DBD::Teradata::impl::err($sessno);
		$DBD::Teradata::errstr = DBD::Teradata::impl::errstr($sessno);
		return undef;
	}
	if ($rc <= 0) {
		return $rc;
	}

	my $ftypes = $sth->{'TYPE'};
	my $fprec = $sth->{'PRECISION'};
	my $fscale = $sth->{'SCALE'};
	my $stmthash = $$stmtinfo[$stmtno];
	my $actends = $$stmthash{'EndsAt'};
	my $actstarts = $$stmthash{'StartsAt'};
	my $actsumstarts = $$stmthash{'SummaryStarts'};
	my $actsumends = $$stmthash{'SummaryEnds'};
	my $issum = $$stmthash{'IsSummary'};
	my $numflds = (defined($$stmthash{'IsSummary'})) ? 
		$$actsumends[$issum] - $$actsumstarts[$issum] + 1 :
		$actends - $actstarts + 1;

	my $ibytes = DBD::Teradata::impl::indicSize($numflds);
	my @row = (undef) x ($sth->{NUM_OF_FIELDS});
		
	if (defined($rawmode)) {
		if (defined($colary)) {
			return $sth->_set_fbav(\@row);
		}
		if ($rawmode eq 'RecordMode') {
			$data = substr($data, $ibytes);
		}
		$row[0] = pack("S a* c", length($data), $data, 10);
		return $sth->_set_fbav(\@row);
	}
	
	my $indstr = substr($data, 0, $ibytes);
	my @indics = DBD::Teradata::impl::getIndics($indstr);
	$data = substr($data, $ibytes);

	my $fpos = (defined($issum)) ? $$actsumstarts[$issum] : $actstarts;
	if (!defined($colary)) {
		$tmpary[0] = $data;
	}
	my $loopcnt = (defined($ary) ? scalar(@$ary) : 1);
	for (my $k = 0; $k < $loopcnt; $k++) {
		$data = $tmpary[$k];
		for (my $i = 0; $i < $numflds; $i++, $fpos++) {
			my $ibit = DBD::Teradata::impl::isIndicSet(\@indics, $i);
			$row[$fpos] = undef; 
			if (($$ftypes[$fpos] eq SQL_VARCHAR) || 
				($$ftypes[$fpos] eq SQL_VARBINARY) ||
				($$ftypes[$fpos] eq SQL_LONGVARBINARY)) {
				($data, $row[$fpos]) = getVarData($data, $$fprec[$fpos], $ibit, $sth);
			}
			elsif (($$ftypes[$fpos] eq SQL_CHAR) || 
				($$ftypes[$fpos] eq SQL_BINARY)) {
				($data, $row[$fpos]) = getFixData($data, $$fprec[$fpos], $ibit, $sth);
			}
			elsif ($$ftypes[$fpos] eq SQL_FLOAT) {
				($data, $row[$fpos]) = getFloat($data, $$fprec[$fpos], $ibit, $sth);
			}
			elsif ($$ftypes[$fpos] eq SQL_DECIMAL) {
				($data, $row[$fpos]) = getDecimal($data, $$fprec[$fpos], 
					$$fscale[$fpos], $ibit, $sth);
			}
			elsif (($$ftypes[$fpos] eq SQL_INTEGER) ||
				($$ftypes[$fpos] eq SQL_DATE)) {
				($data, $row[$fpos]) = getIntDate($data, $$fprec[$fpos], $ibit, $sth);
			}
			elsif ($$ftypes[$fpos] eq SQL_SMALLINT) {
				($data, $row[$fpos]) = getSmallInt($data, $$fprec[$fpos], $ibit, $sth);
			}
			elsif ($$ftypes[$fpos] eq SQL_TINYINT) {
				($data, $row[$fpos]) = getTinyInt($data, $$fprec[$fpos], $ibit, $sth);
			}

			if (defined($$colary[$i])) {
				$ary = $$colary[$i];
				$$ary[$k] = $row[$fpos];
			}
		}
	}
	return $sth->_set_fbav(\@row);
}
*fetchrow_arrayref = \&fetch;

sub getVarData {
	my ($data, $prec, $ibit, $sth) = @_;

	my $flen = unpack("S", $data); 
	my $val;
	$data = substr($data, 2);
	if (($flen != 0) && (!$ibit)) {
		$val = unpack("a$flen", $data);
		if (defined($sth->FETCH('ChopBlanks')) && 
			($sth->FETCH('ChopBlanks') != 0)) {
			$val=~s/\s+$//;
		}
	}
	if (length($data) > $flen) {
		$data = substr($data, $flen);
	}
	else { $data = ''; }
	return ($data, $val);
}

sub getFixData {
	my ($data, $prec, $ibit, $sth) = @_;
	my $val;
	if (!$ibit) {
		$val = unpack("a$prec", $data);
		if (defined($sth->FETCH('ChopBlanks')) && 
			($sth->FETCH('ChopBlanks') != 0)) {
			$val =~ s/\s+$//;
		}
	}
	if (length($data) > $prec) {
		$data = substr($data, $prec);
	}
	else { $data = ''; }
	return ($data, $val);
}

sub getFloat {
	my ($data, $prec, $ibit, $sth) = @_;
	my $val;
	if (!$ibit) {
		$val = unpack("d", $data);
	}
	$data = substr($data, 8);
	return ($data, $val);
}

sub getDecimal {
	my ($data, $prec, $scale, $ibit, $sth) = @_;
	my $val;
	if (!$ibit) {
		$val = DBD::Teradata::impl::cvt_dec2flt($data, 
			$prec, $scale);
	}
	my $decsz = 8;
	if ($prec <= 2) {
		$decsz = 1;
	}
	elsif ($prec <=  4) {
		$decsz = 2;
	}
	elsif ($prec <=  9) {
		$decsz = 4;
	}
	$data = substr($data, $decsz);
	return ($data, $val);
}

sub getIntDate {
	my ($data, $prec, $ibit, $sth) = @_;
	my $val;
	if (!$ibit) {
		$val = unpack("l", $data);
	}
	$data = substr($data, 4);
	return ($data, $val);
}

sub getSmallInt {
	my ($data, $prec, $ibit, $sth) = @_;
	my $val;
	if (!$ibit) {
		$val = unpack("s", $data);
	}
	$data = substr($data, 2);
	return ($data, $val);
}

sub getTinyInt {
	my ($data, $prec, $ibit, $sth) = @_;
	my $val;
	if (!$ibit) {
		$val = unpack("c", $data);
	}
	$data = substr($data, 1);
	return ($data, $val);
}

sub STORE {
	my ($sth, $attr, $val) = @_;
	if ($attr =~ /^tdat_/) {
		$sth->{$attr} = $val;
		return 1;
	}
	return $sth->SUPER::STORE($attr, $val);
}

sub FETCH {
	my($sth, $attr) = @_;
	if ($attr =~ /^tdat_/) {
		return $sth->{$attr};
	}
	return $sth->SUPER::FETCH($attr);
}

sub rows { 
	my($sth) = @_; 
	$sth->FETCH('tdat_rows');
}

sub finish {
	my($sth) = @_;
	DBD::Teradata::impl::finish($sth->FETCH('tdat_sessno'));
	$sth->SUPER::finish;
	1;
}

sub DESTROY {
	undef;
}

sub errstr {
	my($sth) = @_;
	my $sessno = $sth->FETCH('tdat_sessno');
	return DBD::Teradata::impl::errstr($sessno);
}

sub err {
	my($sth) = @_;
	my $sessno = $sth->FETCH('tdat_sessno');
	return DBD::Teradata::impl::err($sessno);
}

sub ProcDataInfo {
	my ($sth, $pcl, $stmtno) = @_;
	my $flds = unpack('S', $pcl);
	$pcl = substr($pcl, 2);
	$flds *= 2;
	my $descr = "S$flds";
	$flds /= 2;
	my @diflds = unpack($descr, $pcl);

	my @fname = ();
	my @fname_uc = ();
	my @fname_lc = ();
	my @ftype = ();
	my @ftitle = ();
	my @fformat = ();
	my @fprec = ();
	my @fscale = ();
	my @fnullable = ();

	my $i = 0;	
	for ($i = 0; $i < $flds; $i++) {
		if ($diflds[$i * 2] > 900) {
			my $t = $diflds[$i*2];
			$diflds[$i*2] = (($t & 0xFF)<<8) + ($t>>8);
			$t = $diflds[($i*2) + 1];
			$diflds[($i*2) + 1] = (($t & 0xFF)<<8) + ($t>>8);
		}
		if (!defined($ptypemap{($diflds[($i * 2)] & tdat_NULL_MASK)})) {
			last;
		}
		$ftype[$i] = $ptypemap{($diflds[($i * 2)] & tdat_NULL_MASK)};
		$fnullable[$i] = ($diflds[($i * 2)] & 1);
		my $len = $diflds[(($i * 2)+1)];
		if ($ftype[$i] == SQL_DECIMAL) {
			$fprec[$i] = int($len/256);
			$fscale[$i] = $len%256;
		}
		else { 
			$fprec[$i] = $len;
			$fscale[$i] = 0; 
		}
		$fname[$i] = '';
		$fname_uc[$i] = '';
		$fname_lc[$i] = '';
		$ftitle[$i] = '';
		$fformat[$i] = '';
	}

	$sth->{NAME} = \@fname;
	$sth->{NAME_lc} = \@fname_lc;
	$sth->{NAME_uc} = \@fname_uc;
	$sth->{TYPE} = \@ftype;
	$sth->{PRECISION} = \@fprec;
	$sth->{SCALE} = \@fscale;
	$sth->{NULLABLE} = \@fnullable;
	$sth->{tdat_TITLE} = \@ftitle;
	$sth->{tdat_FORMAT} = \@fformat;
	
	my $stmtinfo = $sth->{tdat_stmt_info};
	$$stmtinfo[$stmtno] = {
		ActivityType => "PMPC",
		ActivityCount => 0,
		Warning => undef,
		StartsAt => 0,
		EndsAt => ($i-1),
		IsSummary => undef,
		SummaryStarts => undef,
		SummaryEnds => undef
	};

	return 1;
}

1;


package DBD::Teradata::impl;

use strict;
use DBI;
use DBI::DBD;
use DBI qw(:sql_types);
use DBD::Teradata qw($phdfltsz %ptypeszs %ptypecodes %ppackstr %ptypemap);

use IO::Socket;
use Socket;
use constant COPFORMATATT_3B2  => 7;
use constant COPFORMATINTEL8086  => 8;
use constant COPDISCARDTEST => 0 ;
use constant COPECHOTEST => 1 ;
my @pclstrings = (
'Unknown',
'PclREQUEST',
'PclRUNSTARTUP',
'PclDATA',
'PclRESP',
'PclKEEPRESP',
'PclABORT',
'PclCANCEL',
'PclSUCCESS',
'PclFAILURE',
'PclRECORD',
'PclENDSTATEMENT',
'PclENDREQUEST ',
'PclFMREQ',
'PclFMRUNSTARTUP',
'PclVALUE',
'PclNULLVALUE',
'PclOK',
'PclFIELD',
'PclNULLFIELD',
'PclTITLESTART',
'PclTITLEEND',
'PclFORMATSTART',
'PclFORMATEND',
'PclSIZESTART',
'PclSIZEEND',
'PclSIZE',
'PclRECSTART',
'PclRECEND',
'PclPROMPT',
'PclENDPROMPT',
'PclREWIND',
'PclNOP',
'PclWITH',
'PclPOSITION',
'PclENDWITH',
'PclLOGON',
'PclLOGOFF',
'PclRUN',
'PclRUNRESP',
'PclUCABORT',
'PclHOSTSTART',
'PclCONFIG',
'PclCONFIGRESP ',
'PclSTATUS',
'PclIFPSWITCH',
'PclPOSSTART',
'PclPOSEND',
'PclBULKRESP',
'PclERROR',
'PclDATE',
'PclROW',
'PclHUTCREDBS',
'PclHUTDBLK',
'PclHUTDELTBL',
'PclHUTINSROW',
'PclHUTRBLK',
'PclHUTSNDBLK',
'PclENDACCLOG',
'PclHUTRELDBCLK',
'PclHUTNOP',
'PclHUTBLD',
'PclHUTBLDRSP',
'PclHUTGETDDT',
'PclHUTGETDDTRSP',
'PclHUTIDX',
'PclHUTIDXRSP',
'PclFIELDSTATUS',
'PclINDICDATA',
'PclINDICREQ',
'Unknown',
'PclDATAINFO',
'PclIVRUNSTARTUP',
'Unknown',
'Unknown',
'Unknown',
'Unknown',
'Unknown',
'Unknown',
'Unknown',
'Unknown',
'Unknown',
'Unknown',
'Unknown',
'Unknown',
'PclOPTIONS',
'PclPREPINFO',
'Unknown',
'PclCONNECT',
'PclLSN',
'PclCOMMIT',
'Unknown',
'Unknown',
'Unknown',
'Unknown',
'Unknown',
'Unknown',
'Unknown',
'Unknown',
'Unknown',
'PclASSIGN',
'PclASSIGNRSP',
'Unknown',
'Unknown',
'Unknown',
'PclERRORCNT',
'PclSESSINFO',
'PclSESSINFORESP',
'Unknown',
'Unknown',
'Unknown',
'Unknown',
'Unknown',
'Unknown',
'PclSESSOPT',
'PclVOTEREQUEST',
'PclVOTETERM',
'PclCMMT2PC',
'PclABRT2PC',
'PclFORGET',
'PclCURSORHOST',
'PclCURSORDBC',   
'PclFLAGGER');



use constant BADPARCEL => 306;
my @activity_types = (
'Unknown',
'Select',
'Insert',
'Update',
'Update..RETRIEVING',
'Delete',
'Create Table',
'Modify Table',
'Create View',
'Create Macro',
'Drop Table',
'Drop View',
'Drop Macro',
'Drop Index',
'Rename Table',
'Rename View',
'Rename Macro',
'Create Index',
'Create Database',
'Create User',
'Grant',
'Revoke',
'Give',
'Drop Database',
'Modify Database',
'Database',
'Begin Transaction',
'End Transaction',
'Abort',
'Null',
'Execute',
'Comment Set',
'Comment Returning',
'Echo',
'Replace View',
'Replace Macro',
'Checkpoint',
'Delete Journal',
'Rollback',
'Release Lock',
'HUT Config',
'Verify Checkpoint',
'Dump Journal',
'Dump',
'Restore',
'RollForward',
'Delete Database',
'internal use only',
'internal use only',
'Show',
'Help',
'Begin Loading',
'Checkpoint Load',
'End Loading',
'Insert'
);
use constant tdat_VARCHAR => 448 ;
use constant tdat_CHAR => 452 ;
use constant tdat_LONG_VARCHAR => 456;
use constant tdat_FLOAT => 480;
use constant tdat_DECIMAL => 484;
use constant tdat_INTEGER => 496;
use constant tdat_SMALLINT => 500;
use constant tdat_VARBYTE => 688;
use constant tdat_BYTE => 692;
use constant tdat_LONGVARBYTE => 696;
use constant tdat_DATE => 752;
use constant tdat_BYTEINT => 756;
use constant tdat_TIMESTAMP => 760;
use constant tdat_TIME => 764;
my %tdtypestrs = ( 448, 'VARCHAR', 452, 'CHAR', 456, 'LONG VARCHAR', 
	480, 'FLOAT', 484, 'DECIMAL', 496, 'INTEGER', 500, 'SMALLINT',
	688, 'VARBYTE', 692, 'BYTE', 696, 'LONGVARBYTE', 752, 'DATE', 
	756, 'BYTEINT', 700, 'TIMESTAMP', 704, 'TIME');
use constant tdat_NULL_MASK => 0xfffe;
my %uptypestrs = ( 448, 'a', 452, 'A', 456, 'a', 480, 'd', 484, 'd', 
	496, 'i', 500, 's', 688, 'a', 692, 'A', 696, 'a', 752, 'I', 756, 'c', 700, 'a*', 704, 'a*');

my %uparmsz = ( 448, 32000, 452, 32000, 456, 32000, 480, 8, 484, 8, 
	496, 4, 500, 2, 688, 32000, 692, 32000, 696, 32000, 752, 4, 756, 1, 760, 20, 764, 8);

my @indicbits = (128, 64, 32, 16, 8, 4, 2, 1);

my @decscales = ( 1.0, 1.0E-1, 1.0E-2, 1.0E-3, 1.0E-4, 1.0E-5, 1.0E-6,
	1.0E-7, 1.0E-8, 1.0E-9, 1.0E-10, 1.0E-11, 1.0E-12,
	1.0E-13, 1.0E-14, 1.0E-15, 1.0E-16, 1.0E-17, 1.0E-18);
	
my @decfactors = ( 1.0, 1.0E1, 1.0E2, 1.0E3, 1.0E4, 1.0E5, 1.0E6,
	1.0E7, 1.0E8, 1.0E9, 1.0E10, 1.0E11, 1.0E12,
	1.0E13, 1.0E14, 1.0E15, 1.0E16, 1.0E17, 1.0E18);
	
my @typewords = ( 'CHAR', 'VARCHAR', 'BYTE', 'VARBYTE', 'INT',
	'FLOAT', 'SMALLINT', 'BYTEINT', 'DEC', 'DATE', 'TIMESTAMP',
	'INTERVAL', 'GRAPHIC', 'VARGRAPHIC','TIME');

my %typeval = ( 
'CHAR', SQL_CHAR, 
'VARCHAR', SQL_VARCHAR, 
'BYTE', SQL_BINARY, 
'VARBYTE', SQL_VARBINARY,
'INT', SQL_INTEGER,
'SMALLINT', SQL_SMALLINT,
'BYTEINT', SQL_TINYINT,
'FLOAT', SQL_FLOAT,
'DEC', SQL_DECIMAL,
'DATE', SQL_DATE,
'TIMESTAMP', SQL_TIMESTAMP,
'INTERVAL', SQL_TIMESTAMP,
'GRAPHIC', SQL_BINARY,
'VARGRAPHIC', SQL_VARBINARY,
'TIME', SQL_TIME);

my @sqlwords = (
	'INS ', 'INSERT ', 'UPD ', 'UPDATE ', 'SEL ', 'SELECT ', 
	'DELETE ', 'EXEC ', 'EXECUTE ', 'LOCK ', 'LOCKING '
);

my $MaxRESPSz = 32100;

my %sesmap;
my %sesfns;
my %sesstate;
my %sesauth;
my %sesauthx;
my %sesinxact;
my %curreq;
my %curresp;
my %lasterr;
my %lastemsg;
my %sesbuff;
my %sescrypt;
my $platform;
my $hostchars;
my $reqfrags = 0;
my $no2bufs = 1;
my $debug = 0;

sub init {
	$curreq{0} = 0;
	$sesauthx{0} = 0;

	$platform = $ENV{'TDAT_PLATFORM_CODE'};
	$hostchars = 127;
	if (!defined($platform)) {
		$platform = COPFORMATINTEL8086;
		my $testval = pack('s', 1234);
		my $netval = unpack('n', $testval);
		if ($netval == 1234) { 
			$platform = COPFORMATATT_3B2; 
		}
	}
	if ($platform == COPFORMATATT_3B2) {
		$hostchars = 255; 
	}

	my $phsz = $ENV{'TDAT_PH_SIZE'};
	if (defined($phsz) && ($phsz > 0) && ($phsz < 1024)) { $phdfltsz = $phsz; }
	
	$reqfrags = $ENV{'TDAT_REQ_FRAG'};
	if (!defined($reqfrags)) { $reqfrags = 0; }

	$no2bufs = $ENV{'TDAT_NO2BUFS'};
	if (!defined($no2bufs)) { $no2bufs = 1; }

	$debug = $ENV{'TDAT_DBD_DEBUG'};
	if (!defined($debug)) { $debug = 0; }

	DBI->trace_msg("DBD::Teradata init: platform = $platform, debug = $debug,\n" .
		"charset = $hostchars, ph size = $phdfltsz, request fragment = $reqfrags\n" .
		"response no dbl buf = $no2bufs\n", 1);
	return 1;
}

sub cleanup {
	my $sessno = pop(@_);

	undef $sesmap{$sessno};
	undef $sesstate{$sessno};
	undef $sesinxact{$sessno};
	undef $curreq{$sessno};
	undef $curresp{$sessno};
	undef $sesauth{$sessno};
	undef $sesauthx{$sessno};
	1;
}
sub buildtdhdr {
	my($kind, $len, $sessno, $authent) = @_;

	my $hostbyte = 0;
	my $charset = $hostchars;

	if ($kind == 1) { $hostbyte = $platform; $charset = 0; }
	elsif ($kind == 9) { $hostbyte = COPECHOTEST; }

	if ($kind == 3) { $curreq{$sessno} = 0; }
	elsif (($kind == 5) || ($kind == 8)) {
		$curreq{$sessno}++; 
	}
	my $reqmsg = '';
	$reqmsg = pack("C6 S n LS L N CSCN N C16",
		3,
		1,
		$kind,
		0,
		0,
		$hostbyte,
		0,
		$len,
		0,0,
		0,

		$sessno,
		0,
		0,
		$sesauthx{$sessno},
		$authent,
		$curreq{$sessno},
		0,
		$charset,
		0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0);

	return $reqmsg;
}

sub tdsend {
	my ($connfd, $msg, $dummy) = @_;
	my $n = 0;
	if ($debug) { DBI->trace_msg("Sending request\n", 2); }

	if ($reqfrags == 0) {
		$n = send($connfd, $msg, 0);
		if (!defined($n)) { 
			DBI->trace_msg("Can't send: $!\n", 1);
			return undef; 
		}
		if ($n != length($msg)) { 
			DBI->trace_msg("Only sent $n bytes!\n", 1);
			return undef;
		}
		if ($debug) { DBI->trace_msg("Request sent\n", 2); }
		return length($msg);
	}
	$n = send($connfd, substr($msg, 0, 52), 0);
	if (!defined($n)) { 
		DBI->trace_msg("Can't send hdr: $!\n", 1);
		return undef; 
	}
	if ($n != 52) { 
		DBI->trace_msg("Only sent $n bytes of hdr!\n", 1);
		return undef;
	}
	if (length($msg) > 52) {
		$n = send($connfd, substr($msg, 52), 0);
		if (!defined($n)) { 
			DBI->trace_msg("Can't send body: $!\n", 1);
			return undef; 
		}
		if ($n != length($msg) - 52) { 
			DBI->trace_msg("Only sent $n bytes of body!\n", 1);
			return undef;
		}
	}
	if ($debug) { DBI->trace_msg("Request sent\n", 2); }
	return length($msg);
}
sub gettdresp {
	my($connfd, $sessno) = @_;
	my $rspmsg = '';
	my $hdrlen = 52;
	my $rsplen = $MaxRESPSz + 100;
	my $hdr = '';
	if ($debug) { DBI->trace_msg("Rcving Header\n", 2); }

	while ($hdrlen > 0) {
		if (!recv($connfd, $rspmsg, $rsplen, 0)) {
			if ((!defined($rspmsg)) || (length($rspmsg) == 0)) {
				$hdrlen = length($rspmsg);
				DBI->trace_msg("System error: can\'t recv msg header $!;\n rcvd $hdrlen bytes.\n", 2);
			
				$lasterr{$sessno} = 1199;
				$lastemsg{$sessno} = "System error: can\'t recv msg header; closing connection.";
				close($connfd);
				return '';
			}
		}
		$hdrlen -= length($rspmsg);
		$hdr .= $rspmsg;
		$rspmsg = '';
	}
	if ($debug) { DBI->trace_msg(hexdump('Rsp COPReq header', $hdr), 2); }
	my ($tdver, $tdclass, $tdkind, $tdenc, $tdchksum, $tdbytevar, 
		$tdwordvar, $tdmsglen) = unpack("C6Sn", $hdr);
	if ($tdmsglen < 0) { $tdmsglen += 65536; }
	if (($tdver !=  3) || (($tdclass & 0x7f) !=  2)) {
		DBI->trace_msg("Invalid response message header; closing connection.", 2);
		$lasterr{$sessno} = 1200;
		$lastemsg{$sessno} = "Invalid response message header; closing connection.";
		close($sesmap{$sessno});
		cleanup($sessno);
		return '';
	}
	$rspmsg = '';
	if (length($hdr) > 52) {
		$tdmsglen -= (length($hdr) - 52);
	}
	$rspmsg = $hdr;
	my $tmsg;
	while ($tdmsglen > 0) {
		if (!recv($connfd, $tmsg, $tdmsglen, 0)) {
			if ((!defined($tmsg)) || (length($tmsg) == 0)) {
				$hdrlen = length($tmsg);
				DBI->trace_msg("System error: can\'t recv msg body$!;\n rcvd $hdrlen bytes.\n", 2);
				$lasterr{$sessno} = 1199;
				$lastemsg{$sessno} = "System error: can't recv() msg body; closing connection.";
				close($connfd);
				return '';
			}
		}
		my $lrspmsg = length($tmsg);
		
		if ($lrspmsg < $tdmsglen) {
			if ($debug) { DBI->trace_msg("GOT $lrspmsg BYTES, NEEDED $tdmsglen\n",2); }
		}
		$rspmsg .= $tmsg;
		$tdmsglen -= $lrspmsg;
		$tmsg = '';
	}
	if ($debug) { DBI->trace_msg(hexdump('Rsp COPSeg and body', $rspmsg), 2); }
	if (($sessno == 0) || ($tdkind == 11) || ($tdkind == 12))
	{ return $rspmsg; }

	my ($tdsess, $tdauth1, $tdauth2, $reqno) = unpack("N LL L", substr($rspmsg, 20));

	if ($tdsess !=  $sessno) {
		close($sesmap{$sessno});
		cleanup($sessno);
		$lasterr{$sessno} = 1201;
		$lastemsg{$sessno} = "Message for unknown session $tdsess recv'd; closing connection.";
		return '';
	}

	if (unpack('C', substr($rspmsg, 1, 1)) == 0x82) {
		$rspmsg = decrypt($sessno, $rspmsg);
	}
	$rspmsg = substr($rspmsg, 52);
	if ($debug) { DBI->trace_msg(pcldump($rspmsg), 1); }
	my ($tdflavor, $tdlen) = unpack("SS", $rspmsg);
	if (($tdflavor == 9) || ($tdflavor == 49)) {
		my ($tdstmt, $tdinfo, $tderr, $tdelen) = 
			unpack("SSSS", substr($rspmsg, 4));
		my $tdemsg = substr($rspmsg, 12, $tdelen);
		DBI->trace_msg("ERROR\: $tdemsg\n", 2);
		$lasterr{$sessno} = $tderr;
		$lastemsg{$sessno} = $tdemsg;
	}
	$sesstate{$sessno} = 2;
	if (($no2bufs == 0) && (($tdkind == 5) || ($tdkind == 6)) &&
		(pclwalk($rspmsg))) {
		tdcontinue($sessno, 1);
	}
	return $rspmsg;
}
sub ping {
	my $sessno = pop(@_);
	my $reqmsg = buildtdhdr(9, 0, 0, 0);
	tdsend($sesmap{$sessno}, $reqmsg, 0) || 
		(close($sesmap{$sessno}) && return 0);

	my $rspmsg = gettdresp($sesmap{$sessno}, 0);
	if ($rspmsg eq '') { return 0; }
	return 1;
}
sub isIndicSet {
	my($ibytes, $fldnum) = @_;
	my $ipos = int($fldnum/8);
	return ($$ibytes[$ipos] & $indicbits[$fldnum%8]);
}

sub setIndicator {
	my($ibytes, $fldnum) = @_;
	my $ipos = int($fldnum/8);
	$$ibytes[$ipos] |= $indicbits[$fldnum%8];
	return $ibytes;
}
sub indicSize {
	my($fldcnt) = @_;
	my $ibytes = int($fldcnt/8);
	if ($fldcnt%8 !=  0) { $ibytes++; }
	return $ibytes;
}

sub initIndic {
	my($fldcnt) = @_;
	my $ibytes = int($fldcnt/8);
	my @indics;
	if ($fldcnt%8 !=  0) { $ibytes++; }
	for (my $i = 0; $i < $ibytes; $i++) {
		$indics[$i] = 0;
	}
	return @indics;
}

sub cvtIndics {
	my ($ibits) = @_;
	my $j = scalar(@$ibits);
	my $pbits = chr(0) x $j;
	foreach my $i (0..$j-1) {
		substr($pbits, $i, 1) = pack("C", $ibits->[$i]);
	}
	return $pbits;
}

sub getIndics {
	my ($ibits) = @_;
	my $j = length($ibits);
	my @pbits =();
	foreach my $i (0..$j-1) {
		push @pbits, unpack("C", substr($ibits, $i, 1));
	}
	return @pbits;
}
sub tddo {
	my($sessno, $dbreq, $dbdata) = @_;

	my $reqlen = 4 + 10 + 4 + length($dbreq) + 6;
	my $reqmsg = buildtdhdr(5, $reqlen, $sessno, 
		$sesauth{$sessno});
	if ($sesauth{$sessno} == 0xffffffff) { 
		$sesauthx{$sessno}++; $sesauth{$sessno} = 0; 
	}
	else { $sesauth{$sessno}++; }

	$reqmsg .= pack("SSa10 SSA* SSS", 
		85,
		14,
		'RE',

		1,
		4 + length($dbreq),
		$dbreq,

		4,
		6,
		$MaxRESPSz);
	
	tdsend($sesmap{$sessno}, $reqmsg, 0) || return undef;

	my $rspmsg = gettdresp($sesmap{$sessno}, $sessno);
	if ($rspmsg eq '') { return undef; }
	my ($tdflavor, $tdlen) = unpack("SS", $rspmsg);

	if (($tdflavor == 9) || ($tdflavor == 49)) {
		my ($tdstmt, $tdinfo, $tderr, $tdelen) = 
			unpack("SSSS", substr($rspmsg, 4));
		my $tdemsg = substr($rspmsg, 12, $tdelen);
		DBI->trace_msg("\nERROR\: $tdemsg\n", 2);
		$lasterr{$sessno} = $tderr;
		$lastemsg{$sessno} = $tdemsg;
		$sesinxact{$sessno} = 0;
	}
	if (($tdflavor !=  8) || ($tdlen < 4)) {
		return undef;
	}
	my ($stmtno, $rowcnt, $warncode, $fldcount, $activity, $warnlen) = 
		unpack("SLSSSS", substr($rspmsg, 4));
	if ($warnlen != 0) {
		$lasterr{$sessno} = $warncode;
		$lastemsg{$sessno} = unpack("a$warnlen", substr($rspmsg, 18));
	}

	if ($debug) { DBI->trace_msg("Session $sessno executed $dbreq\n", 1); }

	return $rowcnt;
}
sub tdcontinue {
	my ($sessno, $nowait) = @_;

	if ($sesstate{$sessno} != 1) {
		my $reqmsg = buildtdhdr(6, 6, $sessno, $sesauth{$sessno});
		if ($sesauth{$sessno} == 0xffffffff) { 
			$sesauthx{$sessno}++; $sesauth{$sessno} = 0; 
		}
		else { $sesauth{$sessno}++; }

		$reqmsg .= pack("SSS", 4, 6, $MaxRESPSz);
		tdsend($sesmap{$sessno}, $reqmsg, 0) || return undef;

		$sesstate{$sessno} = 1;
		if ($debug) { DBI->trace_msg("Session $sessno continued\n", 1); }
		if ($nowait != 0) { return ''; }
	}

	my $rspmsg = gettdresp($sesmap{$sessno}, $sessno);
	$sesstate{$sessno} = 2;
	if ($rspmsg eq '') { return undef; }
	return $rspmsg;
}
sub connect {
	my($dbsys, $port, $username, $password, $dbname, $errcode, $errstr) = @_;
	$$errcode = 0;
	$$errstr = '';
	
	my $reqlen = 4 + 32 + 4;
	my $authent = int(rand(time()));
	my $reqmsg = buildtdhdr(1, $reqlen, 0, $authent);
	$reqmsg .= pack("SSA32SS", 
		100,
		4+32,
		$username,
		
		42, 4);

	my $connfd = newsocket();
	if (!defined($connfd)) { 
		$$errcode = -1;
		$$errstr = "Unable to allocate a socket.";
		return undef; 
	}

	my $dest = sockaddr_in($port, inet_aton($dbsys));
	if (! $dest) {
		$$errcode = -1;
		$$errstr = "Unable to get host address.";
		close($connfd);
		return undef;
	}
	if (! connect($connfd, $dest)) {
		$$errcode = -1;
		$$errstr = "Unable to connect: $!";
		close($connfd);
		return undef;
	}
	setsockopt($connfd, SOL_SOCKET, SO_KEEPALIVE, pack("l", 1));
	if ($debug) { DBI->trace_msg(hexdump('Request Msg', $reqmsg), 2); }
	if (! tdsend($connfd, $reqmsg, 0)) {
		$$errcode = -1;
		$$errstr = "Send failed: $!";
		close($connfd);
		return undef;
	}

	my $rspmsg = gettdresp($connfd, 0);
	if ($rspmsg eq '') { 
		$$errcode = -1;
		$$errstr = "Recv failed: $!";
		close($connfd);
		return undef; 
	}
	my ($sessno, $tdauth1, $tdauth2, $reqno) = unpack("N LL L", substr($rspmsg, 20));
	if ($sessno == 0) {
		$rspmsg = substr($rspmsg, 52);
		my ($tdflavor, $tdlen) = unpack("SS", $rspmsg);
		if (($tdflavor == 9) || ($tdflavor == 49)) {
			my ($tdstmt, $tdinfo, $tderr, $tdelen) = 
				unpack("SSSS", substr($rspmsg, 4));
			$$errcode = $tderr;
			$$errstr = substr($rspmsg, 12, $tdelen);
			DBI->trace_msg("ERROR\: $$errstr\n", 2);
		}
		close($connfd);
		undef $curreq{$sessno};
		return undef;
	}

	$sesfns{fileno($connfd)} = $sessno;
	$rspmsg = substr($rspmsg, 52);
	my ($tdflavor, $tdlen, $pbkey, $sesaddr, $pubkeyn, $relary, $verary, 
		$hostid)  = unpack("SSA8A32A32A6A14S", $rspmsg);

	if (($tdflavor !=  101) || ($tdlen !=  98)) {
		close($connfd);
		undef $curreq{$sessno};
		$$errcode = 1202;
		$$errstr = 
			"Unknown response parcel $tdflavor recv'd during ASSIGN; closing connection.";
		return undef;
	}

	$sesauth{$sessno} = $authent+1;
	$sesauthx{$sessno} = 0;
	$sesbuff{$sessno} = '';
	if ($debug) { DBI->trace_msg("Session $sessno assigned for Rel $relary Vers $verary\n", 1); }
	my ($major_ver, $minor_ver) = ($verary=~/^(\d+)[A-Z]?\.(\d+)/);
	$curreq{$sessno} = 0;
	if (($major_ver > 5) || (($major_ver == 5) && ($minor_ver > 0))) {
		if (!gen_key($connfd, $sessno)) {
			$$errcode = -1;
			$$errstr = "Can't generate encryption key.";
			return undef;
		}
	}
	my $conmsg = pack("SSA*",
		36,
		4+length($username)+length($password)+1,
		$username . ',' . $password);
	my $lgnsrc = "?????        $$  ????  PERL  01  LSS";
	$conmsg .= pack("SSA4C6 SSA16LSS SSA*",
		114,
		14,
		'TNND',0,0,0,0,0,0,
		
		88,
		4+24, 'DBC/SQL', 
			0,
			0,
			0,
		3, length($lgnsrc)+4, $lgnsrc);
		
	$reqlen = length($conmsg);
	$reqmsg = buildtdhdr(3, $reqlen, $sessno, $sesauth{$sessno});
	$reqmsg .= $conmsg;
	
	if ($debug) { DBI->trace_msg(hexdump('Request Msg', $reqmsg), 2); }
	if (($major_ver > 5) || (($major_ver == 5) && ($minor_ver > 0))) {
		$reqmsg = encrypt($sessno, $reqmsg);
	}

	if (! tdsend($connfd, $reqmsg, 0)) {
		close($connfd);
		$$errcode = -1;
		$$errstr = "Send failed: $!";
		return undef;
	}

	$rspmsg = gettdresp($connfd,$sessno);

	if ($rspmsg eq '') { return 0; }

	($tdflavor, $tdlen, $pbkey, $sesaddr, $pubkeyn, $relary, $verary, 
		$hostid)  = unpack("SSC8C32C32A6A14S", $rspmsg);

	if (($tdflavor == 9) || ($tdflavor == 49)) {
		my ($tdstmt, $tdinfo, $tderr, $tdelen) = 
			unpack("SSSS", substr($rspmsg, 4));
		$$errcode = $tderr;
		$$errstr = substr($rspmsg, 12, $tdelen);
		DBI->trace_msg("ERROR\: $$errstr\n", 2);
		close($connfd);
		undef $curreq{$sessno};
		undef $sesauth{$sessno};
		undef $sesauthx{$sessno};
		return undef;
	}
	if ($tdflavor !=  8) {
		close($connfd);
		$$errcode = -1;
		$$errstr = "Unexpected parcel $tdflavor.";
		undef $curreq{$sessno};
		undef $sesauth{$sessno};
		undef $sesauthx{$sessno};
		return undef;
	}

	if ($debug) { DBI->trace_msg("Session $sessno connected\n", 1); }
	$sesmap{$sessno} = $connfd;
	if ((!defined($dbname)) || ($dbname eq '')) {
		return $sessno;
	}
	my $dbreq = "DATABASE " . $dbname;

	tddo($sessno, $dbreq, '');
	$sesinxact{$sessno} = 0;
	return $sessno;
}
sub disconnect {
	my $sessno = pop(@_);
	my $reqmsg = buildtdhdr(8, 4, $sessno, $sesauth{$sessno});
	$reqmsg .= pack("SS", 37, 4);
	
	tdsend($sesmap{$sessno}, $reqmsg, 0);
	if ($debug) { DBI->trace_msg("Logged off session $sessno\n", 1); }
	close($sesmap{$sessno});
	cleanup($sessno);
	return 1;
}
sub cvt_dec2flt {
	my($decstr, $prec, $scale) = @_;
	my $dval = 0.0;
	my @ival = (0,0);
	if ($prec <= 2) {
		$ival[0] = unpack("c", $decstr);
		return ($ival[0] * $decscales[$scale]);
	}
	if ($prec <=  4) {
		$ival[0] = unpack("s", $decstr);
		return ($ival[0] * $decscales[$scale]);
	}
	if ($prec <=  9) {
		$ival[0] = unpack("l", $decstr);
		return ($ival[0] * $decscales[$scale]);
	}
	if ($platform == COPFORMATATT_3B2) {
		@ival = unpack("l L", $decstr);
		return (($ival[0]*(2.0**32)) + $ival[1]) * $decscales[$scale];
	}
	else {
		@ival = unpack("L l", $decstr);
		return (($ival[1]*(2.0**32)) + $ival[0]) * $decscales[$scale];
	}
}

sub cvt_flt2dec {
	my($dval, $prec, $scale) = @_;
	my $decstr = '';
	$dval = int($dval * $decfactors[$scale]);
	if ($prec <= 2) {
		$decstr = pack("c", $dval);
		return $decstr;
	}
	if ($prec <=  4) {
		$decstr = pack("s", $dval);
		return $decstr;
	}
	if ($prec <=  9) {
		$decstr = pack("l", $dval);
		return $decstr;
	}
	my $ival1 = int($dval/(2**32));
	my $ival2 = int($dval - ($ival1*(2.0**32)));
	if ($platform == COPFORMATATT_3B2) {
		$decstr = pack("l L", $ival1, $ival2);
	}
	else {
		$decstr = pack("L l", $ival2, $ival1);
	}
	return $decstr;
}

sub parse_using {
	my ($req, $typeary, $typelen) = @_;
	$req = "\U$req\E";
	$req = ' ' . $req;
	if ($req!~/\sUSING[\s\(]/) { return 0; }
	my $word;
	foreach $word (@sqlwords) {
		if ($req=~/USING\s*\(.*\)\s*$word/) {
			$req=~s/\)\s*$word/ $word/;
			$req =~s/\sUSING\s*\(/ USING /;
			last;
		}
	}
	my $reqstart = index($req, ' USING');
	$req =~s/(\S)\,/$1 \,/g;
	$req =~s/\,(\S)/\, $1/g;
	$req =~s/(\S)\(/$1 \(/g;
	$req =~s/\sCASESPECIFIC/ /g;
	$req =~s/\sNOT\s/ /g;
	$req =~s/\sLONG\s+VARCHAR/ VARCHAR/g;
	$req =~s/\sLONG\s+VARBYTE/ VARBYTE/g;
	$req =~s/\sLONG\s+VARGRAPHIC/ VARGRAPHIC/g;
	$req =~s/\sCHAR\s+VARYING/ VARCHAR/g;
	$req =~s/DOUBLE\s+PRECISION/FLOAT/g;
	$req =~s/\sNUMERIC\s/ DEC /g;
	$req =~s/\sREAL\s/ FLOAT /g;
	$req =~s/\sCHARACTER\s/ CHAR /g;
	$req =~s/\sINTEGER\s/ INT /g;
	$req =~s/\sDECIMAL\s/ DEC /g;
	my $reqend = length($req);
	foreach $word (@sqlwords) {
		if (index($req, " $word", $reqstart) == -1) { next; }
		if ($reqend > index($req, $word, $reqstart)) { 
			$reqend = index($req, $word, $reqstart); 
		}
	}
	$req = substr($req, $reqstart, $reqend-$reqstart);
	$req =~s/ USING //;
	$req =~s/\(\s/\(/g;
	$req =~s/\s\)/\)/g;
	$req =~s/\((\d+)\s*\,\s*(\d+)\)/\($1\;$2\)/g;
	$req =~s/\s\((\d+)/\($1/g;
	my @reqdecs = split(',', $req);
	my $decl = '';
	my $usingstr = '';
	my $typecnt = 0;
	foreach $decl (@reqdecs) {
		if ($decl=~/\S+\s+(\S+)/) { 
			$decl = $1;
			$decl=~s/^\((.+)\)$/$1/;
			if ($decl=~/(\w+)\(([^\)]+)\)/) { 
				$decl = $1; 
				my $decsz = $2;
				if ($decsz=~/(\d+)\;(\d+)/) {
					my $tprec = $1;
					my $tscale = $2;
					$$typelen[$typecnt] = ($tprec * 256) + $tscale;
				}
				else {
					if ($decl eq 'DEC') {
						$$typelen[$typecnt] = $decsz * 256;
					}
					else {
						$$typelen[$typecnt] = $decsz;
					}
				}
			}
			else {
				$$typelen[$typecnt] = 0;
				if ($decl eq 'CHAR') { 
					$$typelen[$typecnt] = 1;
				}
				elsif ($decl eq 'VARCHAR') { 
					$$typelen[$typecnt] = 32000;
				}
				elsif ($decl eq 'VARBYTE') { 
					$$typelen[$typecnt] = 32000;
				}
			}
			if (!defined($typeval{$decl})) { return -1; }
			$$typeary[$typecnt] = $typeval{$decl};
			$typecnt++;
		}
	}
	return $typecnt;
}
sub prepare {
	my($sessno, $dbreq, $fname, $ftype, $fprec, $fscale, $fnullable, $ptypes, 
		$plens, $usephs, $acttype, $actcount, $actwarns,
		$actstarts, $actends, $actsumstarts, $actsumends, $ftitle, $fformat) = @_;
	$lasterr{$sessno} = 0;
	$lastemsg{$sessno} = '';
	if ($dbreq=~/^\s*HELP\s*/i) {
		$lasterr{$sessno} = -1010;
		$lastemsg{$sessno} = 'Failure -1010: HELP statement not supported.';
		return undef;
	}
	my $tmpreq = $dbreq;
	$tmpreq=~s/\'\'//g;
	$tmpreq =~s/\'.+\'//g;

	my $phcnt = 0;
	my $x = 0;
	my $datainfo = '';
	$$usephs = ($tmpreq =~ tr/\?//);
	if ($$usephs !=  0) {
		$datainfo = pack('S', $$usephs);
		for (my $i = 0; $i < $$usephs; $i++) {
			$datainfo .= pack('SS', 449, $phdfltsz);
			$$ptypes[$i] = SQL_VARCHAR;
			$$plens[$i] = $phdfltsz;
		}
	}
	my $usingvars = parse_using($tmpreq, $ptypes, $plens);
	if ($usingvars == -1) {
		$lasterr{$sessno} = 500;
		$lastemsg{$sessno} = "Failure 500: Invalid USING clause.";
		return undef; 
	}
	if (($$usephs > 0) && ($usingvars > 0)) {
		$lasterr{$sessno} = 501;
		$lastemsg{$sessno} = "Failure 501: Can't mix USING clause and placeholders in same statement.";
		return undef; 
	}
	
	my $reqlen = 4 + 10 + 4 + length($dbreq) + 6;
	if ($datainfo ne '') { $reqlen += length($datainfo) + 4; }
	my $reqmsg = buildtdhdr(5, $reqlen, $sessno, 
		$sesauth{$sessno});
	if ($sesauth{$sessno} == 0xffffffff) { 
		$sesauthx{$sessno}++; 
		$sesauth{$sessno} = 0; 
	}
	else { $sesauth{$sessno}++; }

	if ($datainfo ne '') {
		$reqmsg .= pack("SSa10 SSa* SSA* SSS", 
			85,
			14,
			'RP',

			1,
			4 + length($dbreq),
			$dbreq,

			71,
			4+length($datainfo),
			$datainfo,

			4,
			6,
			$MaxRESPSz);
	}
	else {
		$reqmsg .= pack("SSa10 SSA* SSS", 
			85,
			14,
			'RP',

			1,
			4 + length($dbreq),
			$dbreq,

			4,
			6,
			$MaxRESPSz);
	}
	
	if (!tdsend($sesmap{$sessno}, $reqmsg, 0)) {
		$lasterr{$sessno} = 301;
		$lastemsg{$sessno} = "System error: can't send() PREPARE request.";
		return undef;
	}

	my $rspmsg = gettdresp($sesmap{$sessno}, $sessno);
	if ($rspmsg eq '') { return undef; }
	my ($tdflavor, $tdlen, $tderr, $tdelen);
	my ($stmtno, $rowcnt, $warncode, $fldcount, $activity, $warnlen, $pcl);
	my ($costest, $sumcnt, $colcnt, $datatype, $datalen, $cnmlen, $cname, $cfmt,
		$cfmtlen, $cttllen, $ctitle);
	my $nextcol = 0;
	while ($rspmsg ne '') {
		($tdflavor, $tdlen) = unpack("SS", $rspmsg);

		if (($tdflavor == 11) ||
			($tdflavor == 12)) {
			$rspmsg = substr($rspmsg, $tdlen);
			next;
		}
		if ($tdflavor == 8) {
			($stmtno, $rowcnt, $warncode, $fldcount, $activity, $warnlen) = 
				unpack("SLSSSS", substr($rspmsg, 4));
			$$acttype[$stmtno] = $activity_types[$activity];
			$$actcount[$stmtno] = $rowcnt;
			if ($warnlen != 0) {
				$$actwarns[$stmtno] = "Warning $warncode: " . 
					unpack("a$warnlen", substr($rspmsg, 18));
			}
			$rspmsg = substr($rspmsg, $tdlen);
			next;
		}

		if (($tdflavor == 9) || ($tdflavor == 49)) {
			($stmtno, $rowcnt, $tderr, $tdelen) = 
				unpack("SSSS", substr($rspmsg, 4));
			my $tdemsg = substr($rspmsg, 12, $tdelen);
			DBI->trace_msg("ERROR $tderr\: $tdemsg\n", 2);
			$lasterr{$sessno} = $tderr;
			$lastemsg{$sessno} = (($tdflavor == 9) ? "Failure" : "Error") .
				" $tderr\: $tdemsg on Statement $stmtno.";
				
			$sesstate{$sessno} = 0;
			$sesinxact{$sessno} = 0;
			undef $$actsumstarts[$stmtno];
			undef $$actsumends[$stmtno];
			undef $$actstarts[$stmtno];
			undef $$actends[$stmtno];
			return undef;
		}

		if ($tdflavor !=  86) {
			$lasterr{$sessno} = BADPARCEL;
			$lastemsg{$sessno} = 
				"Invalid parcel stream: got $tdflavor when PREPINFO expected.";
			return undef;
		}

		($tdflavor, $tdlen, $costest, $sumcnt, $colcnt) = 
			unpack("SSdSS", $rspmsg);
		undef $$actsumstarts[$stmtno];
		undef $$actsumends[$stmtno];
		if (($colcnt == 0) && ($sumcnt == 0)) {
			undef $$actstarts[$stmtno];
			undef $$actends[$stmtno];
			$rspmsg = substr($rspmsg, $tdlen);
			next;
		}

		$pcl = substr($rspmsg, 0, $tdlen);
		$rspmsg = substr($rspmsg, $tdlen);
		$pcl = substr($pcl, 16);
		$$actstarts[$stmtno] = $nextcol;
		$$actends[$stmtno] = $nextcol + $colcnt - 1;
		my $nextsum = 0;
		if ($sumcnt != 0) {
			my @sumstart = [];
			my @sumend = [];
			my @sumpos = [];
			$$actsumstarts[$stmtno] = \@sumstart;
			$$actsumends[$stmtno] = \@sumend;
		}
		while (1) {
			for (my $i = 0; $i < $colcnt; $i++, $nextcol++) {
				($datatype, $datalen, $cnmlen) = unpack("SSS", $pcl);
				if ($cnmlen == 0) {
					$cname = 'COLUMN' . int($nextcol);
					$cfmtlen = unpack("S", substr($pcl, 6));
				}
				else {
					($cname, $cfmtlen) = unpack("A$cnmlen S", substr($pcl, 6));
				}
		
				if ($cfmtlen == 0) {
					undef $cfmt;
					$cttllen = unpack("S", substr($pcl, 6+$cnmlen+2));
				}
				else {
					($cfmt, $cttllen) = 
						unpack("A$cfmtlen S", substr($pcl, 6+$cnmlen+2));
				}
		
				if ($cttllen == 0) {
					undef $ctitle;
				}
				else {
					$ctitle = unpack("A$cttllen", 
						substr($pcl, 6+$cnmlen+2+$cfmtlen+2));
				}
				$pcl = substr($pcl, 10 + $cnmlen + $cfmtlen + $cttllen);
				$$fname[$nextcol] = $cname;
				if (($cnmlen == 0) && ($cttllen != 0)) {
					$$fname[$nextcol] = $ctitle;
				}
				else {
					$$fname[$nextcol] = $cname;
				}
				$$ftype[$nextcol] = $ptypemap{$datatype & tdat_NULL_MASK};
				$$fnullable[$nextcol] = $datatype & 1;
				$$ftitle[$nextcol] = $ctitle;
				$cfmt = "\U$cfmt\E";
				if ($cfmt=~/^\-(\-+)(.*)/) {
					my $lcfmt = length($1) + 1;
					$cfmt = "\-($lcfmt)$2";
				}
				$$fformat[$nextcol] = $cfmt;
				if (($$ftype[$nextcol] == SQL_BINARY) &&
					($$ftype[$nextcol] == SQL_VARBINARY)) {
					$$fformat[$nextcol] = "6($$fprec[$nextcol])";
				}

				if ($$ftype[$nextcol] == SQL_DECIMAL) {
					$$fprec[$nextcol] = int($datalen/256);
					$$fscale[$nextcol] = int($datalen%256);
				}
				else { 
					$$fprec[$nextcol] = $datalen;
					$$fscale[$nextcol] = 0; 
				}
	
				if ($$ftype[$nextcol] != SQL_DECIMAL) {
					if ($debug) { DBI->trace_msg("$$fname[$nextcol]\: $$ftype[$nextcol] LENGTH $$fprec[$nextcol]\n", 1); }
				}
				else {
					my $decsz = 8;
					if ($$fprec[$nextcol] <= 2) { $decsz = 1; }
					elsif ($$fprec[$nextcol] <= 4) { $decsz = 2; }
					elsif ($$fprec[$nextcol] <= 9) { $decsz = 4; }

					if ($debug) { DBI->trace_msg("$$fname[$nextcol]\: DECIMAL($$fprec[$nextcol], $$fscale[$nextcol]) LENGTH $decsz\n", 1); }
				}
			}
			if ($sumcnt == 0) {
				last;
			}
			$colcnt = unpack('S', $pcl);
			my $sumstart = $$actsumstarts[$stmtno];
			my $sumend = $$actsumends[$stmtno];
			$$sumstart[$nextsum] = $nextcol;
			$$sumend[$nextsum] = $nextcol + $colcnt - 1;
			$nextsum++;
			$pcl = substr($pcl, 2);
			$sumcnt--;
		}
	}
	if ($debug) { DBI->trace_msg("Session $sessno PREPAREd $dbreq\n", 1); }

	if ($usingvars > 0) { return $usingvars; }
	else { return $$usephs; }
}
sub execute {
	my ($sessno, $stmt, $datainfo, $indicdata, $nowait, 
		$stmtinfo, $stmtno, $rawmode, $keepresp, $sth) = @_;
	$lasterr{$sessno} = 0;
	$lastemsg{$sessno} = '';
	my $reqmsg = '';
	my $modepcl = 68;
	if (defined($rawmode) && ($rawmode eq 'RecordMode')) {
		$modepcl = 3;
	}
	elsif ($indicdata ne '') {
		if ($datainfo ne '') {
			$reqmsg = pack("SSa10 SSA* SSa* SSa* SSS", 
				85,
				14,
				'IE',

				69,
				4 + length($stmt),
				$stmt,

				71,
				length($datainfo) + 4,
				$datainfo,

				$modepcl,
				length($indicdata) + 4,
				$indicdata,

				(($keepresp) ? 5 : 4),
				6,
				$MaxRESPSz);
		}
		else {
			$reqmsg = pack("SSa10 SSA* SSa* SSS", 
				85,
				14,
				'IE',

				69,
				4 + length($stmt),
				$stmt,

				$modepcl,
				length($indicdata) + 4,
				$indicdata,

				(($keepresp) ? 5 : 4),
				6,
				$MaxRESPSz);
		}
	}
	else {
		$reqmsg = pack("SSa10 SSA* SSS", 
			85,
			14,
			'IE',

			69,
			4 + length($stmt),
			$stmt,

			(($keepresp) ? 5 : 4),
			6,
			$MaxRESPSz);
	}
	my $reqlen = length($reqmsg);
	if ($debug) { DBI->trace_msg(pcldump($reqmsg), 2); }
	$reqmsg = buildtdhdr(5, $reqlen, $sessno, $sesauth{$sessno})
		. $reqmsg;
	if ($sesauth{$sessno} == 0xffffffff) { 
		$sesauthx{$sessno}++; 
		$sesauth{$sessno} = 0; 
	}
	else { $sesauth{$sessno}++; }
	if ($debug) { DBI->trace_msg(hexdump('Exec Request', $reqmsg), 2); }
	tdsend($sesmap{$sessno}, $reqmsg, 0) || return undef;
	$sesstate{$sessno} = 1;
	if ($nowait != 0) { return -1; }

	my $rspmsg = gettdresp($sesmap{$sessno}, $sessno);
	if ($rspmsg eq '') { return undef; }
	my ($tdflavor, $tdlen) = unpack("SS", $rspmsg);
	my ($rowcnt, $tderr, $fldcount, $activity, $tdelen, $tdemsg);
	while ($tdflavor !=  10) {
		if ($tdflavor == 12) {
			$sesstate{$sessno} = 0;
			undef $curresp{$sessno};
			return 0;
		}
		if (($tdflavor == 9) || ($tdflavor == 49)) {
			($$stmtno, $rowcnt, $tderr, $tdelen) = 
				unpack("SSSS", substr($rspmsg, 4));
			$tdemsg = substr($rspmsg, 12, $tdelen);
			DBI->trace_msg("ERROR $tderr\: $tdemsg\n", 2);
			$lasterr{$sessno} = $tderr;
			$lastemsg{$sessno} = (($tdflavor == 9) ? "Failure" : "Error") .
				" $tderr\: $tdemsg on Statement $$stmtno.";
			$sesstate{$sessno} = 0;
			$sesinxact{$sessno} = 0;
			return undef;
		}
		if ($tdflavor ==  8) {
			($$stmtno, $rowcnt, $tderr, $fldcount, $activity, $tdelen) = 
				unpack("SLSSSS",  substr($rspmsg, 4));

			my $stmthash = $$stmtinfo[$$stmtno];
			$$stmthash{'ActivityType'} = $activity_types[$activity];
			$$stmthash{'ActivityCount'} = $rowcnt;
			if ($tdelen != 0) {
				$$stmthash{'Warning'} = "Warning $tderr\: " . 
					unpack("a$tdelen", substr($rspmsg, 18));
			}
			else {
				undef $$stmthash{'Warning'};
			}
		}
		elsif ($tdflavor ==  33) {
			my $stmthash = $$stmtinfo[$$stmtno];
			$$stmthash{'IsSummary'} = (unpack("S", substr($rspmsg, 4)) - 1);
			if (!defined($$stmthash{'SummaryPosition'})) {
				my @sumpos = ();
				my @sumstart = ();
				$$stmthash{'SummaryPosition'} = \@sumpos;
				$$stmthash{'SummaryPosStart'} = \@sumstart;
			}
		}
		elsif ($tdflavor ==  35) {
			my $stmthash = $$stmtinfo[$$stmtno];
			undef $$stmthash{'IsSummary'};
		}
		elsif ($tdflavor ==  46) {
			my $stmthash = $$stmtinfo[$$stmtno];
			my $sumpos = $$stmthash{'SummaryPosition'};
			my $sumstart = $$stmthash{'SummaryPosStart'};
			push(@$sumstart, scalar(@$sumpos));
		}
		elsif ($tdflavor ==  34) {
			my $stmthash = $$stmtinfo[$$stmtno];
			my $sumpos = $$stmthash{'SummaryPosition'};
			push(@$sumpos, (unpack("S", substr($rspmsg, 4))));
		}
		elsif ($tdflavor == 11) {
			my $stmthash = $$stmtinfo[$$stmtno];
			undef $$stmthash{'SummaryPosition'};
			undef $$stmthash{'SummaryPosStart'};
		}
		elsif (($tdflavor != 71) && 
			($tdflavor !=  47)) {
			$lasterr{$sessno} = BADPARCEL;
			$lastemsg{$sessno} = "Received bad parcel $tdflavor.";
			$sesstate{$sessno} = 0;
			return undef;
		}
		$rspmsg = substr($rspmsg, $tdlen);
		if ($rspmsg eq '') {
			$rspmsg = tdcontinue($sessno, $nowait); 
			if (!defined($rspmsg)) { return ''; }
			if ($nowait != 0) { return -1; }
		}
		($tdflavor, $tdlen) = unpack("SS", $rspmsg);
	}

	$curresp{$sessno} = $rspmsg;
	return $rowcnt;
}
sub fetch {
	my ($sessno, $nowait, $stmtinfo, $currstmt, $ary, $maxlen, $retstr, $sth) = @_;
	if (!defined($curresp{$sessno})) { 
		return undef; 
	}
	$lasterr{$sessno} = 0;
	$lastemsg{$sessno} = '';

	if (!defined($maxlen)) { $maxlen = 1; }
	$$retstr = '';
	my $stmtno = $$currstmt;
	my $rspmsg = $curresp{$sessno};
	if ($rspmsg eq '') { 
		$curresp{$sessno} = tdcontinue($sessno, $nowait);
		if (!defined($curresp{$sessno})) { return 0; }
		if ($nowait != 0) { return -1; }
		$rspmsg = $curresp{$sessno};
	}
	my ($tdflavor, $tdlen) = unpack("SS", $rspmsg);
	my ($rowcnt, $tderr, $fldcount, $activity, $tdelen, $tdemsg);
	while ($tdflavor !=  10) {
		if ($tdflavor == 12) {
			$sesstate{$sessno} = 0;
			undef $curresp{$sessno};
			return 0;
		}
		if (($tdflavor == 9) || ($tdflavor == 49)) {
			($stmtno, $rowcnt, $tderr, $tdelen) = 
				unpack("SSSS", substr($rspmsg, 4));
			$tdemsg = substr($rspmsg, 12, $tdelen);
			DBI->trace_msg(print "ERROR $tderr\: $tdemsg\n", 2);
			$lasterr{$sessno} = $tderr;
			$lastemsg{$sessno} = (($tdflavor == 9) ? "Failure" : "Error") .
				" $tderr\: $tdemsg on Statement $stmtno.";
			$sesstate{$sessno} = 0;
			$$currstmt = $stmtno;
			$sesinxact{$sessno} = 0;
			return undef;
		}
		if ($tdflavor ==  8) {
			($stmtno, $rowcnt, $tderr, $fldcount, $activity, $tdelen) = 
				unpack("SLSSSS", substr($rspmsg, 4));

			my $stmthash = $$stmtinfo[$stmtno];
			$$stmthash{'ActivityType'} = $activity_types[$activity];
			$$stmthash{'ActivityCount'} = $rowcnt;
			if ($tdelen != 0) {
				$$stmthash{'Warning'} = "Warning $tderr\: " . 
					unpack("a$tdelen", substr($rspmsg, 18));
			}
			else {
				undef $$stmthash{'Warning'};
			}
			$$currstmt = $stmtno;
		}
		elsif ($tdflavor ==  33) {
			my $stmthash = $$stmtinfo[$$currstmt];
			$$stmthash{'IsSummary'} = (unpack("S", substr($rspmsg, 4)) - 1);
			if (!defined($$stmthash{'SummaryPosition'})) {
				my @sumpos = ();
				my @sumstart = ();
				$$stmthash{'SummaryPosition'} = \@sumpos;
				$$stmthash{'SummaryPosStart'} = \@sumstart;
			}
		}
		elsif ($tdflavor ==  35) {
			my $stmthash = $$stmtinfo[$$currstmt];
			undef $$stmthash{'IsSummary'};
		}
		elsif ($tdflavor ==  46) {
			my $stmthash = $$stmtinfo[$$stmtno];
			my $sumpos = $$stmthash{'SummaryPosition'};
			my $sumstart = $$stmthash{'SummaryPosStart'};
			push(@$sumstart, scalar(@$sumpos));
		}
		elsif ($tdflavor ==  34) {
			my $stmthash = $$stmtinfo[$stmtno];
			my $sumpos = $$stmthash{'SummaryPosition'};
			push(@$sumpos, (unpack("S", substr($rspmsg, 4))));
		}
		elsif ($tdflavor ==  11) {
			my $stmthash = $$stmtinfo[$stmtno];
			undef $$stmthash{'SummaryPosStart'};
			undef $$stmthash{'SummaryPosition'};
		}
		elsif (($tdflavor !=  71) &&
			($tdflavor !=  47)) {
			$lasterr{$sessno} = BADPARCEL;
			$lastemsg{$sessno} = "Received bad parcel $tdflavor.";
			$sesstate{$sessno} = 0;
			return undef;
		}
		$rspmsg = substr($rspmsg, $tdlen);
		if ($rspmsg eq '') {
			$rspmsg = tdcontinue($sessno, $nowait); 
			if (!defined($rspmsg)) { return ''; }
			if ($nowait != 0) { return -1; }
		}
		($tdflavor, $tdlen) = unpack("SS", $rspmsg);
	}

	if (defined($ary)) {
		my $i = 0;
		for ($i = 0; (($i < $maxlen) && ($rspmsg ne '') && ($tdflavor == 10)); $i++) {
		 	$$ary[$i] = pack('Sa*c', $tdlen-4, substr($rspmsg, 4, $tdlen - 4), 10);
		 	$rspmsg = substr($rspmsg, $tdlen);
			($tdflavor, $tdlen) = unpack("SS", $rspmsg);
		}
		$curresp{$sessno} = $rspmsg;
		return $i;
	}
	$$retstr = substr($rspmsg, 4, $tdlen - 4);
	if (length($rspmsg) > $tdlen) {
		$curresp{$sessno} = substr($rspmsg, $tdlen);
	}
	else { $curresp{$sessno} = ''; }
	return 1;
}

sub commit {
	my $sessno = pop(@_);
	return tddo($sessno, 'ET');
}

sub rollback {
	my $sessno = pop(@_);
	return tddo($sessno, 'ABORT');
}

sub err {
	my ($sessno) = @_;
	return $lasterr{$sessno};
}

sub errstr {
	my ($sessno) = @_;
	return $lastemsg{$sessno};
}
sub finish {
	my ($sessno) = @_;

	if ((!defined($sesstate{$sessno})) ||
		($sesstate{$sessno} == 0)) { return 1; }
	my $reqmsg = buildtdhdr(6, 4, $sessno, $sesauth{$sessno});
	if ($sesauth{$sessno} == 0xffffffff) { 
		$sesauthx{$sessno}++; $sesauth{$sessno} = 0; 
	}
	else { $sesauth{$sessno}++; }

	$reqmsg .= pack("SS", 7, 4);
	
	tdsend($sesmap{$sessno}, $reqmsg, 0) || 
		(close($sesmap{$sessno}) && return undef);

	my $rspmsg = gettdresp($sesmap{$sessno}, $sessno);
	if ($rspmsg eq '') { return -1; }
	my ($tdflavor, $tdlen) = unpack("SS", $rspmsg);
	return 1;
}

sub hexdump {
	my($hdr, $buf) = @_;
	my $i = 0;
	my $hexbuf = '';
	my $alphabuf = '';
	my $cval = '';
	my $outstr = "$hdr\:\n";
	for ($i = 0; $i < length($buf); $i++) {
		if ($i%16  == 0) {
			$outstr .= "$hexbuf	$alphabuf\n";
			$hexbuf = ''; $alphabuf = '';
		}
		$hexbuf .= ' ' . unpack("H2", substr($buf, $i, 1));
		$cval = unpack('C', substr($buf, $i, 1));
		if (($cval > 127) || ($cval < 32)) {
			$alphabuf .= '.';
		} else {
			$alphabuf .= chr($cval);
		}
	}
	if (length($hexbuf) > 0) { $outstr .= "$hexbuf	$alphabuf\n"; }
	$outstr .= "\n";
	return $outstr;
}

sub pcldump {
	my($buf) = @_;
	my $i = 0;
	my $hexbuf = '';
	my $alphabuf = '';
	my ($flavor, $pcllen) = (0,0);
	my $cval;
	my $outstr = '';
	while (length($buf) > 0) {
		($flavor, $pcllen) = unpack('SS', $buf);
		if ($flavor < 123) {
			$outstr .= "Parcel $pclstrings[$flavor] length $pcllen\:\n";
		}
		else {
			$outstr .= "Unknown parcel $flavor length $pcllen\:\n";
		}
		$buf = substr($buf, 4);
		$pcllen -= 4;
		for ($i = 0; $i < $pcllen; $i++) {
			if ($i%16  == 0) {
				$outstr .= "$hexbuf	$alphabuf\n";
				$hexbuf = ''; $alphabuf = '';
			}
			$hexbuf .= ' ' . unpack("H2", substr($buf, $i, 1));
			$cval = unpack('C', substr($buf, $i, 1));
			if (($cval > 127) || ($cval < 32)) {
				$alphabuf .= '.';
			} else {
				$alphabuf .= chr($cval);
			}
		}
		if (length($hexbuf) > 0) { $outstr .= "$hexbuf	$alphabuf\n"; }
		$hexbuf = ''; $alphabuf = '';
		$buf = substr($buf, $pcllen);
	}
	return $outstr;
}
sub pclwalk {
	my($buf) = @_;
	my ($flavor, $pcllen) = (0,0);
	while (length($buf) > 0) {
		($flavor, $pcllen) = unpack('SS', $buf);
		if ($flavor == 12) { return undef; }
		$buf = substr($buf, $pcllen);
	}
	return 1;
}

sub FirstAvailable {
	my ($sesslist, $timeout) = @_;
	my $i = 0;
	my $rmask = '';
	my $wmask = '';
	my $emask = '';
	my ($rout, $wout, $eout);
	my $sessno = 0;
	if ($timeout == -1) { $timeout = undef; }
	foreach $sessno (@$sesslist) {
		if ($sesstate{$sessno} == 1) { 
			vec($rmask, fileno($sesmap{$sessno}), 1) = 1;
			$i++;
		}
	}
	if ($i == 0) { 
		return undef; 
	}
	$wmask = 0;
	$emask = $rmask;
	my $n = select($rout=$rmask, undef, $eout=$emask, $timeout);
	if ($n <= 0) { 
		return undef; 
	}

	foreach $i (@$sesslist) {
		if (vec($rout, fileno($sesmap{$i}), 1) == 1) {
			return $i;
		}
	}
	foreach $i (@$sesslist) {
		if (vec($eout, fileno($sesmap{$i}), 1) == 1) {
			return $i;
		}
	}
	return undef;
}

sub FirstAvailList {
	my ($sesslist, $timeout) = @_;
	my $i = 0;
	my $rmask = '';
	my $wmask = '';
	my $emask = '';
	my ($rout, $wout, $eout);
	my $sessno = 0;
	if ($timeout == -1) { $timeout = undef; }
	foreach $sessno (@$sesslist) {
		if ($sesstate{$sessno} == 1) { 
			vec($rmask, fileno($sesmap{$sessno}), 1) = 1;
			$i++;
		}
	}
	if ($i == 0) { 
		return undef; 
	}
	$wmask = 0;
	$emask = $rmask;
	my $n = select($rout=$rmask, undef, $eout=$emask, $timeout);
	if ($n <= 0) { 
		return undef; 
	}

	my @avails = ();
	foreach $i (@$sesslist) {
		if (vec($rout, fileno($sesmap{$i}), 1) == 1) {
			push(@avails, $i);
		}
	}
	if (scalar(@avails) != 0) { return @avails; }
	foreach $i (@$sesslist) {
		if (vec($eout, fileno($sesmap{$i}), 1) == 1) {
			push(@avails, $i);
		}
	}
	if (scalar(@avails) != 0) { return @avails; }
	return undef;
}

sub Realize {
	my ($sessno, $stmtinfo, $stmtno) = @_;
	my $rspmsg = gettdresp($sesmap{$sessno}, $sessno);
	if ($rspmsg eq '') { return undef; }
	my ($tdflavor, $tdlen) = unpack("SS", $rspmsg);
	my ($rowcnt, $tderr, $fldcount, $activity, $tdelen, $tdemsg);
	while ($tdflavor !=  10) {
		if ($tdflavor == 12) {
			$sesstate{$sessno} = 0;
			undef $curresp{$sessno};
			return 0;
		}
		if (($tdflavor == 9) || ($tdflavor == 49)) {
			($$stmtno, $rowcnt, $tderr, $tdelen) = 
				unpack("SSSS", substr($rspmsg, 4));
			$tdemsg = substr($rspmsg, 12, $tdelen);
			DBI->trace_msg("ERROR $tderr\: $tdemsg\n", 2);
			$lasterr{$sessno} = $tderr;
			$lastemsg{$sessno} = (($tdflavor == 9) ? "Failure" : "Error") .
				" $tderr\: $tdemsg on Statement $$stmtno.";
			$sesstate{$sessno} = 0;
			$sesinxact{$sessno} = 0;
			return undef;
		}
		if ($tdflavor ==  8) {
			($$stmtno, $rowcnt, $tderr, $fldcount, $activity, $tdelen) = 
				unpack("SLSSSS",  substr($rspmsg, 4));

			my $stmthash = $$stmtinfo[$$stmtno];
			$$stmthash{'ActivityType'} = $activity_types[$activity];
			$$stmthash{'ActivityCount'} = $rowcnt;
			if ($tdelen != 0) {
				$$stmthash{'Warning'} = "Warning $tderr\: " . 
					unpack("a$tdelen", substr($rspmsg, 18));
			}
			else {
				undef $$stmthash{'Warning'};
			}
		}
		elsif ($tdflavor ==  33) {
			my $stmthash = $$stmtinfo[$$stmtno];
			$$stmthash{'IsSummary'} = (unpack("S", substr($rspmsg, 4)) - 1);
		}
		elsif ($tdflavor ==  35) {
			my $stmthash = $$stmtinfo[$$stmtno];
			undef $$stmthash{'IsSummary'};
		}
		elsif (($tdflavor ==  46) ||
			($tdflavor ==  34) ||
			($tdflavor ==  47)) {
		}
		elsif (($tdflavor != 11) &&
			($tdflavor != 71)) {
			$lasterr{$sessno} = BADPARCEL;
			$lastemsg{$sessno} = "Received bad parcel $tdflavor.";
			$sesstate{$sessno} = 0;
			return undef;
		}
		$rspmsg = substr($rspmsg, $tdlen);
		if ($rspmsg eq '') {
			$rspmsg = tdcontinue($sessno, 1); 
			if (!defined($rspmsg)) { return ''; }
			return -1;
		}
		($tdflavor, $tdlen) = unpack("SS", $rspmsg);
	}

	$curresp{$sessno} = $rspmsg;
	return $rowcnt;
}

sub newsocket {
	local *FH;
	if (!socket(FH, PF_INET, SOCK_STREAM, getprotobyname('tcp'))) {
		return undef;
	}
	return *FH;
}

sub append_buf {
	my ($sessno, $mode, $row) = @_;
	$sesbuff{$sessno} .= 
		pack('SS', (($mode eq 'IndicatorMode') ? 68 : 3), (length($row)+4)) . 
			$row;
	return length($sesbuff{$sessno});
}

sub get_buf_len {
	my ($sessno) = @_;
	return length($sesbuff{$sessno});
}

sub clear_buf {
	my ($sessno) = @_;
	$sesbuff{$sessno} = '';
	1;
}

sub gen_key {
	my ($connfd, $sessno) = @_;

	eval {
		require Crypt::Blowfish;
	};
	
	if ($@) {
		eval {
			require Crypt::Blowfish_PP;
		};
	}
	return undef
		if $@;
	
	eval {
		require Math::BigInt;
	};

	return undef
		if $@;

	my $platpack = (($platform == 8) ? 'L' : 'V') . '13';
 	my $cryptpack = ($platform == 8) ? 'N' : 'V';

	my $reqmsg = buildtdhdr(11, 4, $sessno, $sesauth{$sessno});
	if ($sesauth{$sessno} == 0xffffffff) { 
		$sesauthx{$sessno}++; $sesauth{$sessno} = 0; 
	}
	else { $sesauth{$sessno}++; }
	substr($reqmsg, 52, 4) = pack('SS', 130, 4);
		
	return undef 
		unless defined(tdsend($connfd, $reqmsg));

	my $resp = gettdresp($connfd, $sessno);
	return undef unless $resp;
	
	return undef
		unless ((unpack('C', substr($resp, 2, 1)) == 11)
			&& (unpack('S', substr($resp, 52, 2)) == 131));

	my $len = unpack('S', substr($resp, 52+2, 2)) - 4;
	my $can_blowfish;
	
	foreach my $c (unpack("C$len", substr($resp, 52+4, $len))) {
		$can_blowfish = 1, last
			if ($c == 8);
	}
	return undef unless $can_blowfish;

	my $reqkey = chr(0) x 64;

	substr($reqkey, 0, 10) = pack('C10', 5,1,0,0,1,0,0,0,0,0);

    my $reqlen = 4 + 4 + 64;
    $reqmsg = buildtdhdr(12, $reqlen, $sessno, $sesauth{$sessno});
	if ($sesauth{$sessno} == 0xffffffff) { 
		$sesauthx{$sessno}++; $sesauth{$sessno} = 0; 
	}
	else { $sesauth{$sessno}++; }
    substr($reqmsg, 52, $reqlen) = 
    	pack('SS C C S a*', 
    		132, $reqlen,
    		8,
    		0,
    		64,
    		$reqkey);

	return undef
		unless tdsend($connfd, $reqmsg);
		
	$resp = gettdresp($connfd, $sessno);
	return undef unless $resp;
	
	return undef 
		unless (unpack('S', substr($resp, 52, 2)) == 134);

	$len = unpack('S', substr($resp, 52+2, 2));
	my $authdata = chr(0) x 52;

	$authdata = pack($platpack, 
		qw(5282 5124 2631 706 1578 118 1343 3538 2929 4852 1340 5594 7306));

    $reqlen = 4 + 4 + 52;
    $reqmsg = buildtdhdr(12, $reqlen, $sessno, $sesauth{$sessno});
	if ($sesauth{$sessno} == 0xffffffff) { 
		$sesauthx{$sessno}++; $sesauth{$sessno} = 0; 
	}
	else { $sesauth{$sessno}++; }
    substr($reqmsg, 52, $reqlen) = 
    	pack('SS C C S a*', 
    		132, $reqlen,
    		8,
    		2,
    		52,
    		$authdata);

	return undef
		unless tdsend($connfd, $reqmsg);
		
	$resp = gettdresp($connfd, $sessno);
	return undef unless $resp;
	
	return undef 
		unless (unpack('S', substr($resp, 52, 2)) == 134);
	$authdata = substr($resp, 52 + 10, 52);
	my $keydata = chr(0) x 52;
	my @bigkeys = unpack($platpack, $authdata);
	$bigkeys[$_] = Math::BigInt->new($bigkeys[$_])
		foreach (0..12);

	my @b = qw(
	3866 1858 5067 1269 4428 1729 2339 7469 3867 6737 473 5595 1465
	);

	$bigkeys[$_]->bmodpow($b[$_], 7919)
		foreach (0..12);
	$sescrypt{$sessno} = pack($platpack, @bigkeys);
	return 1;
}

sub encrypt {
	my ($sessno, $req) = @_;

	my $bf = Crypt::Blowfish->new($sescrypt{$sessno});
	my $msglen = (unpack('n', substr($req, 3, 2)) * 65536) +
		unpack('n', substr($req, 8, 2));
	my $enclen = (($msglen + 28 + 7) >> 3) * 8;
	my $enclen2 = $enclen - 28;
	my $pad = $enclen2 + 4 - $msglen;
	$req .= chr(0) x $pad;
	my $lcnt = $enclen2 >> 2;
	my $platpack = (($platform == 8) ? 'L' : 'V') . '13';
 	my $cryptpack = ($platform == 8) ? 'N' : 'V';
	substr($req, 24, (4 * $lcnt)) = 
		pack("$cryptpack$lcnt", unpack("L$lcnt", substr($req, 24, (4 * $lcnt))));
	my $cipher = substr($req, 0, 24);
	my $i = 24;
	$cipher .= $bf->encrypt(substr($req, $i, 8)),
	$i += 8
		while ($i < $enclen + 24);
	substr($cipher, 24, (4 * $lcnt)) = 
		pack("$cryptpack$lcnt", unpack("L$lcnt", substr($cipher, 24, (4 * $lcnt))));
	$cipher .= pack('L', 0);
	$pad = $enclen2 - $msglen;
	$msglen = $enclen2 + 4;
	substr($cipher, $msglen + 52 - 1, 1) = pack('C', $pad);
	substr($cipher, 1, 1) = pack('C', 0x81);

	substr($cipher, 3, 2) = pack('n', (($msglen >> 16) & 0xFFFF));
	substr($cipher, 8, 2) = pack('n', ($msglen & 0xFFFF));

	return $cipher;
}

sub decrypt {
	my ($sessno, $resp) = @_;
	return $resp
		unless (unpack('C', substr($resp, 1, 1)) & 0x80);

	my $bf = Crypt::Blowfish->new($sescrypt{$sessno});

	my $msglen = length($resp) - 52;
	my $pad = unpack('C', substr($resp, length($resp) - 1, 1));

	my $size = length($resp) - 24 - 4;
	
	$size = (($size + 7) >> 3) * 8;
	my $cnt  = $size/4;
	my $platpack = (($platform == 8) ? 'L' : 'V') . '13';
 	my $cryptpack = ($platform == 8) ? 'N' : 'V';
	substr($resp, 24, $size) = 
		pack("$cryptpack$cnt", unpack("L$cnt", substr($resp, 24, $size)));
	my $cleartext = substr($resp, 0, 24) . ('\0' x ($cnt * 4));
	my $offset = 24;
	
	substr($cleartext, $offset, 8) = $bf->decrypt(substr($resp, $offset, 8)),
	$offset += 8
		while ($offset < $size + 24);

	substr($cleartext, 24, $size) = 
		pack("$cryptpack$cnt", unpack("L$cnt", substr($cleartext, 24, $size)));

	$msglen -= $pad;
	$msglen -= 4;
	$cleartext = substr($cleartext, 0, ($msglen + 52));

	substr($cleartext, 1, 1) = 2;
	substr($cleartext, 3, 2) = pack('n', ($msglen >> 16) & 0xFFFF);
	substr($cleartext, 8, 2) = pack('n', ($msglen & 0xFFFF));
	return $cleartext;
}

1;


__END__
=head1 NAME

DBD::Teradata - a DBI driver for Teradata

=head1 SYNOPSIS

  use DBI;

  $dbh = DBI->connect('dbi:Teradata:hostname', 'user', 'password');

See L<DBI> for more information.

=head1 DESCRIPTION

Refer to the included tdatdbd.html.

=head2 *** *BEFORE* BUILDING, TESTING AND INSTALLING this you will need to:

	Build, test and install Perl 5 (minimum version 5.005).
	It is very important to TEST it and INSTALL it!

	Build, test and install the DBI module (minimum version 1.13).
	It is very important to TEST it and INSTALL it!

	Remember to *read* the DBI README, this README, and the included
	tdatdbd.html CAREFULLY! 
    
	I had a lot of info to distill, and POD, though quaint and
	convenient for READMEs like this, just isn't as expressive
	as HTML...hence tdatdbd.html. Please refer to tdatdbd.html for
	detailed usage information.

=head2 *** BUILDING:

	Define the following environment variables:
	(These are used to logon the sessions for the test.pl script.)

		TDAT_DBD_DSN - set to your DBMS's hostname (e.g., 'dbccop1')
		TDAT_DBD_USER - set to the userid that has about 4 MBytes of 
			perm space and can create/drop tables and macros
		TDAT_DBD_PASSWORD - set to the password for aforementioned userid

	For Windows 95/98/2000/ME/NT:
	
	Save us all a great deal of agony, and just copy Teradata.pm
	to your site-specific lib\DBD directory (e.g., \perl\site\lib\DBD
	for most ActiveState configurations) (backup your old copy if you're
	upgrading!!!), and then copy test.pl somewhere and run
	
		perl -w test.pl
		
	and make sure you get all the way to the
	"Tests completed ok, exitting..." output at the end.
	
	For non-Windows:
	
    perl Makefile.pl	# use a perl that's in your PATH
    make
    make test			# will spew results to STDOUT, 
    					# messages to STDERR, 
    					# and create a tdrawtest.out file
    make install (if the tests look okay)

=head2 *** IF YOU HAVE PROBLEMS:

    Please read the tdatdbd.html file which includes important
    information, including tips and workarounds for various
    platform-specific problems.

=head2 *** SUPPORT INFORMATION:

    For the latest DBD::Teradata information, please see

        http://www.presicient.com/tdatdbd

    Bug reports/Comments/suggestions/enhancement requests may be sent to

        darnold@presicient.com

    Please see the following files for more information:

    	tdatdbd.html - the User's Guide
    	
=head2 *** CHANGE HISTORY
	Release 1.20	Sept. 20, 2004
	
		- added logon encryption support
		- removed non-SQL partition support

	Release 1.13	Apr 27, 2003
	
		- updated contact info

	Release 1.12	Dec 10, 2000
	
		- fixed datainfo problem on non-Intel platforms

	Release 1.11	Dec 10, 2000
	
		- added tdat_lsn, tdat_clone, tdat_keepresp, and tdat_utility attributes
		- added support for FASTLOAD, EXPORT, and MONITOR utility sessions
		- added bind_param_inout() function
		- added BindParamArray(), BindColArray(), and FirstAvailList() 
			driver-specific function
		- fixed sth->rows(), and improved dbh->do() behavior
		- improved error reporting on failed DBI->connect() calls

	Release 1.10	Nov 12, 2000
	
		- first official CPAN release

=head2 *** MAILING LISTS

    As a user of a of DBD::Teradata, you need
    to be aware of the following addresses:

    The DBI mailing lists located at

        dbi-announce@perl.org   for announcements
        dbi-dev@perl.org 		for developer/maintainer discussions
        dbi-users@perl.org 		for end user level discussion and help

    To subscribe or unsubscribe to each individual list refer to

        http://dbi.perl.org

    Teradata maintains a bulletin board at
    
    	http://www.teradata.com/teradataforum/
    	
    Refer to this forum for Teradata-specific help and info.

=cut
