#
#   Copyright (c) 2000 Dean Arnold
#
#   You may distribute under the terms of either the GNU General Public
#   License or the Artistic License, as specified in the Perl README file,
#   with the exception that it cannot be placed on a CD-ROM or similar media
#   for commercial distribution without the prior approval of the author.
#
#
require 5.005;
use strict;
use DBI;
use DBI::DBD;

use IO::Socket;

package DBD::Teradata::impl;

use Socket;


#
#	define lots of symbols for our various protocol constants
#
#	Message kinds
#
my $COPKINDASSIGN    = 1;
my $COPKINDREASSIGN =    2;
my $COPKINDCONNECT =  3;
my $COPKINDRECONNECT =  4;
my $COPKINDSTART  = 5;
my $COPKINDCONTINUE =    6;
my $COPKINDABORT    = 7;
my $COPKINDLOGOFF   = 8;
my $COPKINDTEST    = 9;
my $COPKINDCONFIG  = 10;
my $COPKINDDIRECT  = 255;
#
#	Platform format codes
#
my $COPFORMATIBM =  3;
my $COPFORMATHONEYWELL  = 4;
my $COPFORMATATT_3B2  = 7;
my $COPFORMATINTEL8086  = 8;
my $COPFORMATVAX  = 9;
my $COPFORMATUTS  = 10;
#
#	for 2PC; we don't use them...yet ?
#
my $COPDONTCHECKTRANS  = 0;
my $COPCHECKTRANS  = 1;
my $COPNOTINTRANS  = 0;
my $COPINTRANS  = 1;
#
#	echo test constants
#
my $COPDISCARDTEST = 0 ;
my $COPECHOTEST = 1 ;
#
#	Parcel flavors
#
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

my $PclREQUEST    = 1;
my $PclRUNSTARTUP  = 2;
my $PclDATA        = 3;
my $PclRESP        = 4;
my $PclKEEPRESP    = 5;
my $PclABORT       = 6;
my $PclCANCEL      = 7;
my $PclSUCCESS     = 8;
my $PclFAILURE     = 9;
my $PclRECORD      = 10;
my $PclENDSTATEMENT =  11;
my $PclENDREQUEST  = 12;
my $PclFMREQ       = 13;
my $PclFMRUNSTARTUP =  14;
my $PclVALUE       = 15;
my $PclNULLVALUE   = 16;
my $PclOK          = 17;
my $PclFIELD       = 18;
my $PclNULLFIELD   = 19;
my $PclTITLESTART  = 20;
my $PclTITLEEND    = 21;
my $PclFORMATSTART = 22;
my $PclFORMATEND   = 23;
my $PclSIZESTART   = 24;
my $PclSIZEEND     = 25;
my $PclSIZE        = 26;
my $PclRECSTART    = 27;
my $PclRECEND      = 28;
my $PclPROMPT      = 29;
my $PclENDPROMPT   = 30;
my $PclREWIND      = 31;
my $PclNOP         = 32;
my $PclWITH        = 33;
my $PclPOSITION    = 34;
my $PclENDWITH     = 35;
my $PclLOGON       = 36;
my $PclLOGOFF      = 37;
my $PclRUN         = 38;
my $PclRUNRESP     = 39;
my $PclUCABORT     = 40;
my $PclHOSTSTART   = 41;
my $PclCONFIG      = 42;
my $PclCONFIGRESP  = 43;
my $PclSTATUS      = 44;
my $PclIFPSWITCH   = 45;
my $PclPOSSTART    = 46;
my $PclPOSEND      = 47;
my $PclBULKRESP    = 48;
my $PclERROR       = 49;
my $PclDATE        = 50;
my $PclROW         = 51;
my $PclHUTCREDBS   = 52;
my $PclHUTDBLK     = 53;
my $PclHUTDELTBL   = 54;
my $PclHUTINSROW   = 55;
my $PclHUTRBLK     = 56;
my $PclHUTSNDBLK   = 57;
my $PclENDACCLOG = 58;
my $PclHUTRELDBCLK = 59;
my $PclHUTNOP      = 60;
my $PclHUTBLD      = 61;
my $PclHUTBLDRSP   = 62; 
my $PclHUTGETDDT   = 63;
my $PclHUTGETDDTRSP =  64;
my $PclHUTIDx      = 65;
my $PclHUTIDxRsp   = 66;
my $PclFieldStatus = 67;
my $PclINDICDATA   = 68;
my $PclINDICREQ    = 69;
my $PclDATAINFO    = 71;
my $PclIVRUNSTARTUP =  72;
my $PclOPTIONS     = 85;
my $PclPREPINFO    = 86;
my $PclCONNECT     = 88;
my $PclLSN         = 89;
my $PclCOMMIT = 90;
my $PclASSIGN   = 100;
my $PclASSIGNRSP  = 101;

my $PclERRORCNT    = 105;
my $PclSESSINFO    = 106; 
my $PclSESSINFORESP =  107;
my $PclSESSOPT     = 114;
my $PclVOTEREQUEST = 115;
my $PclVOTETERM    = 116;
my $PclCMMT2PC     = 117;
my $PclABRT2PC     = 118;
my $PclFORGET      = 119;
my $PclCURSORHOST   = 120; 
my $PclCURSORDBC    = 121;   
my $PclFLAGGER      = 122;

#
#	Activity type codes
#
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
#
#	Character set codes
#
my $tdat_ASCII = 127;
my $tdat_EBCDIC = 64;
#
#	Datatype codes
#
my $tdat_VARCHAR = 448 ;
my $tdat_CHAR = 452 ;
my $tdat_LONG_VARCHAR = 456;
my $tdat_FLOAT = 480;
my $tdat_DECIMAL = 484;
my $tdat_INTEGER = 496;
my $tdat_SMALLINT = 500;
my $tdat_VARBYTE = 688;
my $tdat_BYTE = 692;
my $tdat_LONGVARBYTE = 696;
my $tdat_DATE = 752;
my $tdat_BYTEINT = 756;
my $tdat_TIMESTAMP = 760;
my $tdat_TIME = 764;
#
#	maps type codes to type names
#
my %tdtypestrs = ( 448, 'VARCHAR', 452, 'CHAR', 456, 'LONG VARCHAR', 
	480, 'FLOAT', 484, 'DECIMAL', 496, 'INTEGER', 500, 'SMALLINT',
	688, 'VARBYTE', 692, 'BYTE', 696, 'LONGVARBYTE', 752, 'DATE', 
	756, 'BYTEINT', 700, 'TIMESTAMP', 704, 'TIME');
#
#	mask to remove the 'nullable' bit
#
my $tdat_NULL_MASK = 0xfffe;
#
#	maps type codes to equivalent Perl 'pack' descriptors
#
my %uptypestrs = ( 448, 'a', 452, 'A', 456, 'a', 480, 'd', 484, 'd', 
	496, 'i', 500, 's', 688, 'a', 692, 'A', 696, 'a', 752, 'I', 756, 'c', 700, 'a*', 704, 'a*');

my %ppackstr = (
	DBI::SQL_VARCHAR, 'Sa', 
	DBI::SQL_CHAR, 'A', 
	DBI::SQL_FLOAT, 'd', 
	DBI::SQL_DECIMAL, 'i2',
	DBI::SQL_INTEGER, 'i', 
	DBI::SQL_SMALLINT, 's', 
	DBI::SQL_TINYINT, 'c', 
	DBI::SQL_VARBINARY, 'Sa',
	DBI::SQL_BINARY, 'a',
	DBI::SQL_LONGVARBINARY, 'Sa',
	DBI::SQL_DATE, 'a8',
	DBI::SQL_TIMESTAMP, 'a20',
	DBI::SQL_TIME, 'a8'
	);
#
#	and the inverse
#
my %ptypecodes = ( 
	DBI::SQL_VARCHAR, 448, 
	DBI::SQL_CHAR, 452, 
	DBI::SQL_FLOAT, 480, 
	DBI::SQL_DECIMAL, 484,
	DBI::SQL_INTEGER, 496, 
	DBI::SQL_SMALLINT, 500, 
	DBI::SQL_TINYINT, 756, 
	DBI::SQL_VARBINARY, 688,
	DBI::SQL_BINARY, 692,
	DBI::SQL_LONGVARBINARY, 696,
	DBI::SQL_DATE, 752,
	DBI::SQL_TIMESTAMP, 760,
	DBI::SQL_TIME, 764
	);

my %ptypeszs = ( 
	DBI::SQL_VARCHAR, 32000, 
	DBI::SQL_CHAR, 32000, 
	DBI::SQL_FLOAT, 8, 
	DBI::SQL_DECIMAL, 8,
	DBI::SQL_INTEGER, 4, 
	DBI::SQL_SMALLINT, 2, 
	DBI::SQL_TINYINT, 1, 
	DBI::SQL_VARBINARY, 32000,
	DBI::SQL_BINARY, 32000,
	DBI::SQL_LONGVARBINARY, 32000,
	DBI::SQL_DATE, 8,
	DBI::SQL_TIMESTAMP, 20,
	DBI::SQL_TIME, 8
	);

my %ptypemap = ( 
	448, DBI::SQL_VARCHAR,
	452, DBI::SQL_CHAR,
	480, DBI::SQL_FLOAT,
	484, DBI::SQL_DECIMAL,
	496, DBI::SQL_INTEGER,
	500, DBI::SQL_SMALLINT, 
	756, DBI::SQL_TINYINT,
	688, DBI::SQL_VARBINARY,
	692, DBI::SQL_BINARY,
	696, DBI::SQL_LONGVARBINARY,
	752, DBI::SQL_DATE,
	760, DBI::SQL_TIMESTAMP,
	764, DBI::SQL_TIME
	);
#
#	maps max sizes to type codes
#
my %uparmsz = ( 448, 32000, 452, 32000, 456, 32000, 480, 8, 484, 8, 
	496, 4, 500, 2, 688, 32000, 692, 32000, 696, 32000, 752, 4, 756, 1, 760, 20, 764, 8);
#
#	CLI-type error codes
#
#	NOTE: only error codes meaningful to this implementation are 
#	included here.
#
my $BADLOGON   = 303;  # invalid logon string         
my $NOSESSION  = 304;  # specified session doesn't  exist                        
my $NOREQUEST  = 305;  # specified req does not  exist
my $BADPARCEL  = 306;  # invalid parcel received      
my $REQEXHAUST = 307; # request data exhausted       
my $NOACTIVE   = 311; # no active sessions           
my $BADID      = 313; # bad indentifier field in logon string                       
my $SESSCRASHED  = 315; # session has crashed          

my $REQOVFLOW  = 350; # Request size exceeds maximum.              

#
#	begin volatile variable definitions;
#	these need thread mutexes for safety
#
my $MaxRESPSz = 32100;

#
#	stmt ctxts contain:
#		sessno - session number
#		skt - socket for session
#		reqno - current request number for session
#		sqlstr - current SQL for session
#		sesauth - current authentication value LSBs
#		sesauthx - current auth. MSB
#		rowdesc - current row description
#		params - current parameter bindings
#		parmtypes - current paramtypes
#		paramsz - current param sizes
#		bindvars - current bind variables
#		lasterr - last error code
#		last emsg - lat err msg string
#
my %sesmap;	# maps sockets to sessno's
my %sesfns;	# maps socket fileno's to sessno's
my %sesstate;	# current state of session:
#				0 => idle (no outstanding request)
#				1 => active (request in progress)
#				2 => ready (request outstanding, but not in progress)
#
my %sesauth;	# LS bytes of authenticator for sessno
my %sesauthx;	# MSB of authenticator for sessno
my %sesinxact;	# 1 => session in active transaction
my %curreq;		# current reqno for sessno
my %curresp;	# current response buffer from DBMS
my %lasterr;	# last err code for sessno
my %lastemsg;	# last error msg for sessno
my %seslsn;		# LSN of the session
my %sespart;	# partition of session
my %sesbuff;	# deferred execution buffer

#
# we can make an educated guess at platform; we'll need a config file or
# environment variable for other than IBM, INTEL, or 3B2/Sun/Motorola/etc.
#
my $platform;
my $hostchars;
my $phdfltsz = 16;
my $reqfrags = 0;
my $no2bufs = 1;
my $debug = 0;

#
#	make this thread safe so only first thread actually effects
#	the global operating params
#
sub init {	# init platform characteristics
	$curreq{0} = 0;
	$sesauthx{0} = 0;

	$platform = $ENV{'TDAT_PLATFORM_CODE'};
	$hostchars = $tdat_ASCII;
	if (ord('A') != 65) {
		$hostchars = $tdat_EBCDIC;
	}
	if (!defined($platform)) {
		$platform = $COPFORMATINTEL8086;
		if ($hostchars == $tdat_EBCDIC) {
			$platform = $COPFORMATIBM; 
		}
		else {
			my $testval = pack('s', 1234);
			my $netval = unpack('n', $testval);
			if ($netval == 1234) { 
				$platform = $COPFORMATATT_3B2; 
			}
		}
	}
	if ($platform == $COPFORMATATT_3B2) {
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

#
#	mutex here to protect the cleanup of the
#	ctxt hashes
#
sub cleanup {	# sessno
	my $sessno = pop(@_);

	undef $sesmap{$sessno};
	undef $sesstate{$sessno};
	undef $sesinxact{$sessno};
	undef $curreq{$sessno};
	undef $curresp{$sessno};
	undef $sesauth{$sessno};
	undef $sesauthx{$sessno};
	undef $seslsn{$sessno};
	undef $sespart{$sessno};
	1;
}
#
#	general purpose function for building msg headers
#
sub buildtdhdr {	# kind, len, sessno, authent
	my($kind, $len, $sessno, $authent) = @_;

	my $hostbyte = 0;
	my $charset = $hostchars;

	if ($kind == $COPKINDASSIGN) { $hostbyte = $platform; $charset = 0; }
	elsif ($kind == $COPKINDTEST) { $hostbyte = $COPECHOTEST; }

	if ($kind == $COPKINDCONNECT) { $curreq{$sessno} = 0; }
	elsif (($kind == $COPKINDSTART) || ($kind == $COPKINDLOGOFF)) {
		$curreq{$sessno}++; 
	}
	my $reqmsg = '';
	$reqmsg = pack("C6 S n LS L L CSCL N C16",
		3, 	# version
		1, 	# class
		$kind, 	# kind
		0, 	# encryption
		0, 	# chksum
		$hostbyte, # bytevar
		0, 	# wordvar
		$len, # msglen
		0,0,	# mbox
		0,	# corrtag

		$sessno,	# sessno
		0,	# auth flag
		0,	# secure bits
		$sesauthx{$sessno}, # auth. counter MSB
		$authent,	# auth. counter
		$curreq{$sessno},	# reqno
		0,	# MBZ
		$charset,	# hostcharset
		0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0);	# resvd

	return $reqmsg;
}

sub tdsend { # sessno, msg
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
#
#	use fragmented request
#
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
#
#	general purpose function for getting response msg
#
sub gettdresp {	# sessno, returns buffer
	my($connfd, $sessno) = @_;
	my $rspmsg = '';
	my $hdrlen = 52;
	my $rsplen = $MaxRESPSz + 100;
	my $hdr = '';
#
#	get fixed part first
#
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
	if ($tdmsglen < 0) { $tdmsglen += 65536; }	# to handle large resp msgs
	if (($tdver !=  3) || ($tdclass !=  2)) {
		DBI->trace_msg("Invalid response message header; closing connection.", 2);
		$lasterr{$sessno} = 1200;
		$lastemsg{$sessno} = "Invalid response message header; closing connection.";
		close($sesmap{$sessno});
		cleanup($sessno);
		return '';
	}
#
#	get the rest
#
	$rspmsg = '';
	if (length($hdr) > 52) {
		$tdmsglen -= (length($hdr) - 52);
	}
	$rspmsg = substr($hdr, 20);
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
#
#	in assign state, leave header intact
#
	if ($sessno == 0) { return $rspmsg; }

	my ($tdsess, $tdauth1, $tdauth2, $reqno) = unpack("L LL L", $rspmsg);

	if ($tdsess !=  $sessno) {
		close($sesmap{$sessno});
		cleanup($sessno);
		$lasterr{$sessno} = 1201;
		$lastemsg{$sessno} = "Message for unknown session $tdsess recv'd; closing connection.";
		return '';
	}
#
#	check for failure/error parcel
#
	$rspmsg = substr($rspmsg, 32);
	if ($debug) { DBI->trace_msg(pcldump($rspmsg), 1); }
	my ($tdflavor, $tdlen) = unpack("SS", $rspmsg);
	if (($tdflavor == $PclFAILURE) || ($tdflavor == $PclERROR)) {
		my ($tdstmt, $tdinfo, $tderr, $tdelen) = 
			unpack("SSSS", substr($rspmsg, 4));
		my $tdemsg = substr($rspmsg, 12, $tdelen);
		DBI->trace_msg("ERROR\: $tdemsg\n", 2);
		$lasterr{$sessno} = $tderr;
		$lastemsg{$sessno} = $tdemsg;
	}
	$sesstate{$sessno} = 2;	# we're ready
	if (($no2bufs == 0) && (($tdkind == $COPKINDSTART) || ($tdkind == $COPKINDCONTINUE)) &&
		(pclwalk($rspmsg))) {
		tdcontinue($sessno, 1);
	}
	return $rspmsg;
}
#
#	send ECHO test msg and wait for reply
#
sub ping { #sessno
	my $sessno = pop(@_);
	my $reqmsg = buildtdhdr($COPKINDTEST, 0, 0, 0);
	tdsend($sesmap{$sessno}, $reqmsg, 0) || 
		(close($sesmap{$sessno}) && return 0);

	my $rspmsg = gettdresp($sesmap{$sessno}, 0);
	if ($rspmsg eq '') { return 0; }
	return 1;
}
#
#	indicator bit manipulators
#
my @indicbits = (128, 64, 32, 16, 8, 4, 2, 1);
sub isIndicSet {	# indicbytes, fieldnum
	my($ibytes, $fldnum) = @_;
	my $ipos = int($fldnum/8);
	return ($$ibytes[$ipos] & $indicbits[$fldnum%8]);
}

sub setIndicator {	# indicbytes, fieldnum
	my($ibytes, $fldnum) = @_;
	my $ipos = int($fldnum/8);
	$$ibytes[$ipos] |= $indicbits[$fldnum%8];
	return $ibytes;
}
#
#	computes num of bytes needed for indicators
#
sub indicSize {	# fieldcount
	my($fldcnt) = @_;
	my $ibytes = int($fldcnt/8);
	if ($fldcnt%8 !=  0) { $ibytes++; }
	return $ibytes;
}

sub initIndic { # fieldcnt, ref indics
	my($fldcnt) = @_;
	my $ibytes = int($fldcnt/8);
	my @indics;
	if ($fldcnt%8 !=  0) { $ibytes++; }
	for (my $i = 0; $i < $ibytes; $i++) {
		$indics[$i] = 0;
	}
	return @indics;
}

sub cvtIndics { # ref indics
	my ($ibits) = @_;
	my $j = scalar(@$ibits);
	my $pbits = pack("C$j", @$ibits);
	return $pbits;
}

sub getIndics { # indics string
	my ($ibits) = @_;
	my $j = length($ibits);
	my @pbits = unpack("C$j", $ibits);
	return @pbits;
}
#
#	simple 'do' function
#
sub tddo {
	my($sessno, $dbreq, $dbdata) = @_;

	my $reqlen = 4 + 10 + 4 + length($dbreq) + 6;
	my $reqmsg = buildtdhdr($COPKINDSTART, $reqlen, $sessno, 
		$sesauth{$sessno});
	if ($sesauth{$sessno} == 0xffffffff) { 
		$sesauthx{$sessno}++; $sesauth{$sessno} = 0; 
	}
	else { $sesauth{$sessno}++; }

	$reqmsg .= pack("SSa10 SSA* SSS", 
		$PclOPTIONS,
		14,	# pclsize
		'RE',	# Request/execute mode

		$PclREQUEST,
		4 + length($dbreq),	# pclsize
		$dbreq,	# request

		$PclRESP,
		6,	# pclsize
		$MaxRESPSz);	# max response size
	
	tdsend($sesmap{$sessno}, $reqmsg, 0) || return undef;

	my $rspmsg = gettdresp($sesmap{$sessno}, $sessno);
	if ($rspmsg eq '') { return undef; }
	my ($tdflavor, $tdlen) = unpack("SS", $rspmsg);

	if (($tdflavor == $PclFAILURE) || ($tdflavor == $PclERROR)) {
		my ($tdstmt, $tdinfo, $tderr, $tdelen) = 
			unpack("SSSS", substr($rspmsg, 4));
		my $tdemsg = substr($rspmsg, 12, $tdelen);
		DBI->trace_msg("\nERROR\: $tdemsg\n", 2);
		$lasterr{$sessno} = $tderr;
		$lastemsg{$sessno} = $tdemsg;
		$sesinxact{$sessno} = 0;
	}
	if (($tdflavor !=  $PclSUCCESS) || ($tdlen < 4)) {
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
#
#	get next batch of rows
#
sub tdcontinue {
	my ($sessno, $nowait) = @_;

	if ($sesstate{$sessno} != 1) {
		my $reqmsg = buildtdhdr($COPKINDCONTINUE, 6, $sessno, $sesauth{$sessno});
		if ($sesauth{$sessno} == 0xffffffff) { 
			$sesauthx{$sessno}++; $sesauth{$sessno} = 0; 
		}
		else { $sesauth{$sessno}++; }

		$reqmsg .= pack("SSS", $PclRESP, 6, $MaxRESPSz);
		tdsend($sesmap{$sessno}, $reqmsg, 0) || return undef;

		$sesstate{$sessno} = 1;	# we're active now
		if ($debug) { DBI->trace_msg("Session $sessno continued\n", 1); }
		if ($nowait != 0) { return ''; }
	}

	my $rspmsg = gettdresp($sesmap{$sessno}, $sessno);
	$sesstate{$sessno} = 2;	# we're ready now
	if ($rspmsg eq '') { return undef; }
	return $rspmsg;
}
#
#	connect to the DBMS
#
sub connect {
	my($dbsys, $port, $username, $password, $dbname, $lsn, $partition, $errcode, $errstr) = @_;
#
#	validate parms
#
	$$errcode = 0;
	$$errstr = '';
	if (!defined($partition)) {
		$partition = 'DBC/SQL';
	}
	elsif (($partition ne 'DBC/SQL') && ($partition ne 'FASTLOAD') && 
		($partition ne 'EXPORT') && ($partition ne 'MONITOR')) {
		$$errcode = -1;
		$$errstr = 'Invalid partition string.';
		return undef;
	}
	if (($partition ne 'DBC/SQL') && ($partition ne 'MONITOR') && 
		((!defined($$lsn)) || ($$lsn == 0))) {
		$$errcode = -1;
		$$errstr = 'LSN required for utility sessions.';
		return undef;
	}
#
#	make sure the LSN is already alloc'd
#
	if ((defined($$lsn)) && ($$lsn != 0)) {
		my $lsnok = 0;
		foreach my $i (keys(%sesmap)) {
			if (($sespart{$i} eq 'DBC/SQL') && 
				($seslsn{$i} == $$lsn)) {
				$lsnok = 1;
				last;
			}
		}
		if (!$lsnok) {
			$$errcode = -1;
			$$errstr = 'Specified LSN not previously allocated.';
			return undef;
		}
	}
	
	my $reqlen = 4 + 32 + 4;
	my $authent = int(rand(time()));
	my $reqmsg = buildtdhdr($COPKINDASSIGN, $reqlen, 0, $authent);
	$reqmsg .= pack("SSA32SS", 
		$PclASSIGN,
		4+32,	# pclsize
		$username,
		
		$PclCONFIG, 4);

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
#		warn "Can't set SO_KEEPALIVE: $!\n";
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
	my ($sessno, $tdauth1, $tdauth2, $reqno) = unpack("L LL L", $rspmsg);
	if ($sessno == 0) {
		$rspmsg = substr($rspmsg, 32);
		my ($tdflavor, $tdlen) = unpack("SS", $rspmsg);
		if (($tdflavor == $PclFAILURE) || ($tdflavor == $PclERROR)) {
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
#
#	should parse config info here so we can limit number of
#	utility session to logon
#
	$sesfns{fileno($connfd)} = $sessno;
	$rspmsg = substr($rspmsg, 32);
	my ($tdflavor, $tdlen, $pbkey, $sesaddr, $pubkeyn, $relary, $verary, 
		$hostid)  = unpack("SSA8A32A32A6A14S", $rspmsg);

	if (($tdflavor !=  $PclASSIGNRSP) || ($tdlen !=  98)) {
		close($connfd);
		undef $curreq{$sessno};
		$$errcode = 1202;
		$$errstr = 
			"Unknown response parcel $tdflavor recv'd during ASSIGN; closing connection.";
		return undef;
	}

	$sesauth{$sessno} = $authent+1;
	$sesauthx{$sessno} = 0;
	$sespart{$sessno} = $partition;
	$seslsn{$sessno} = $$lsn;
	$sesbuff{$sessno} = '';
	if ($debug) { DBI->trace_msg("Session $sessno assigned for Rel $relary Vers $verary\n", 1); }
#
#	returned addr seems to be informational only!!!
#
	my $conmsg = pack("SSA*",
		$PclLOGON,
		4+length($username)+length($password)+1, # pclsize
		$username . ',' . $password);	# username
	my $lgnsrc = " $$  01 LSS";
	$conmsg .= pack("SSA4C6 SSA16LSS SSA*",
		$PclSESSOPT,
		14,
		'TNND',0,0,0,0,0,0,
		
		$PclCONNECT,
		4+24, $partition, 
			(defined($$lsn) ? $$lsn : 0),
			(defined($$lsn) ? (($$lsn == 0) ? 1 : 2) : 0),
			0,
		$PclDATA, length($lgnsrc)+4, $lgnsrc);
		
	$reqlen = length($conmsg);
	$reqmsg = buildtdhdr($COPKINDCONNECT, $reqlen, $sessno, $sesauth{$sessno});
	$reqmsg .= $conmsg;
	
	if ($debug) { DBI->trace_msg(hexdump('Request Msg', $reqmsg), 2); }
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

	if (($tdflavor == $PclFAILURE) || ($tdflavor == $PclERROR)) {
		my ($tdstmt, $tdinfo, $tderr, $tdelen) = 
			unpack("SSSS", substr($rspmsg, 4));
		$$errcode = $tderr;
		$$errstr = substr($rspmsg, 12, $tdelen);
		DBI->trace_msg("ERROR\: $$errstr\n", 2);
		close($connfd);
		undef $curreq{$sessno};
		undef $sesauth{$sessno};
		undef $sesauthx{$sessno};
		undef $sespart{$sessno};
		undef $seslsn{$sessno};
		return undef;
	}
	if ($tdflavor !=  $PclSUCCESS) {
		close($connfd);
		$$errcode = -1;
		$$errstr = "Unexpected parcel $tdflavor.";
		undef $curreq{$sessno};
		undef $sesauth{$sessno};
		undef $sesauthx{$sessno};
		undef $sespart{$sessno};
		undef $seslsn{$sessno};
		return undef;
	}

	if (defined($$lsn) && ($$lsn == 0)) {
		($tdflavor, $tdlen, $$lsn) = unpack("SSL", substr($rspmsg, $tdlen));
		if ($tdflavor != $PclLSN) {
			close($connfd);
			$$errcode = -1;
			$$errstr = "Expected LSN parcel but got $tdflavor.";
			undef $curreq{$sessno};
			undef $sesauth{$sessno};
			undef $sesauthx{$sessno};
			undef $sespart{$sessno};
			undef $seslsn{$sessno};
			return undef;
		}
		$seslsn{$sessno} = $$lsn;
	}

	if ($debug) { DBI->trace_msg("Session $sessno connected\n", 1); }
#
#	maybe send run startup, but for now check for a dbname and set it if
#	needed
#
	$sesmap{$sessno} = $connfd;
	if ((!defined($dbname)) || ($dbname eq '')) {
		return $sessno;
	}
	my $dbreq = "DATABASE " . $dbname;

	tddo($sessno, $dbreq, '');
	$sesinxact{$sessno} = 0;
	return $sessno;
}
#
#	logoff a session
#
sub disconnect {
#
#	logoff session
#
	my $sessno = pop(@_);
	my $reqmsg = buildtdhdr($COPKINDLOGOFF, 4, $sessno, $sesauth{$sessno});
	$reqmsg .= pack("SS", $PclLOGOFF, 4);
	
	tdsend($sesmap{$sessno}, $reqmsg, 0);
	if ($debug) { DBI->trace_msg("Logged off session $sessno\n", 1); }
	close($sesmap{$sessno});
	cleanup($sessno);
	return 1;
}
#
#	conversion functions for DECIMAL types
#	Perl numbers are always stored internally as
#	double precision floats, so we have to do
#	some bit twiddling here...
#
my @decscales = ( 1.0, 1.0E-1, 1.0E-2, 1.0E-3, 1.0E-4, 1.0E-5, 1.0E-6,
	1.0E-7, 1.0E-8, 1.0E-9, 1.0E-10, 1.0E-11, 1.0E-12,
	1.0E-13, 1.0E-14, 1.0E-15, 1.0E-16, 1.0E-17, 1.0E-18);
	
sub cvt_dec2flt { # decstring, precision, scale
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
	if ($platform == $COPFORMATATT_3B2) {
		@ival = unpack("l L", $decstr);
		return (($ival[0]*(2.0**32)) + $ival[1]) * $decscales[$scale];
	}
	else {
		@ival = unpack("L l", $decstr);
		return (($ival[1]*(2.0**32)) + $ival[0]) * $decscales[$scale];
	}
}

my @decfactors = ( 1.0, 1.0E1, 1.0E2, 1.0E3, 1.0E4, 1.0E5, 1.0E6,
	1.0E7, 1.0E8, 1.0E9, 1.0E10, 1.0E11, 1.0E12,
	1.0E13, 1.0E14, 1.0E15, 1.0E16, 1.0E17, 1.0E18);
	
sub cvt_flt2dec { # floatval, precision, scale
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
	if ($platform == $COPFORMATATT_3B2) {
		$decstr = pack("l L", $ival1, $ival2);
	}
	else {
		$decstr = pack("L l", $ival2, $ival1);
	}
	return $decstr;
}
#
#	parse out any using clause
#
my @typewords = ( 'CHAR', 'VARCHAR', 'BYTE', 'VARBYTE', 'INT',
	'FLOAT', 'SMALLINT', 'BYTEINT', 'DEC', 'DATE', 'TIMESTAMP',
	'INTERVAL', 'GRAPHIC', 'VARGRAPHIC','TIME');

my %typeval = ( 
'CHAR', DBI::SQL_CHAR, 
'VARCHAR', DBI::SQL_VARCHAR, 
'BYTE', DBI::SQL_BINARY, 
'VARBYTE', DBI::SQL_VARBINARY,
'INT', DBI::SQL_INTEGER,
'SMALLINT', DBI::SQL_SMALLINT,
'BYTEINT', DBI::SQL_TINYINT,
'FLOAT', DBI::SQL_FLOAT,
'DEC', DBI::SQL_DECIMAL,
'DATE', DBI::SQL_DATE,
'TIMESTAMP', DBI::SQL_TIMESTAMP,
'INTERVAL', DBI::SQL_TIMESTAMP,
'GRAPHIC', DBI::SQL_BINARY,
'VARGRAPHIC', DBI::SQL_VARBINARY,
'TIME', DBI::SQL_TIME);

my @sqlwords = (
	'INS ', 'INSERT ', 'UPD ', 'UPDATE ', 'SEL ', 'SELECT ', 
	'DELETE ', 'EXEC ', 'EXECUTE ', 'LOCK ', 'LOCKING '
);

sub parse_using {	# sql string
	my ($req, $typeary, $typelen) = @_;
#
#	normalize the request to uppercase with single space
#	separators
#
	$req = "\U$req\E";
	$req = ' ' . $req;
	if ($req!~/\sUSING[\s\(]/) { return 0; }	# no USING clause
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
#
#	eliminate stuff we don't care about
#	and normalize the rest
#
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
#
#	extract only the USING clause
#
	my $reqend = length($req);
	foreach $word (@sqlwords) {
		if (index($req, " $word", $reqstart) == -1) { next; }
		if ($reqend > index($req, $word, $reqstart)) { 
			$reqend = index($req, $word, $reqstart); 
		}
	}
	$req = substr($req, $reqstart, $reqend-$reqstart);
	$req =~s/ USING //;
#
#	normalize a bit more
#
	$req =~s/\(\s/\(/g;
	$req =~s/\s\)/\)/g;
	$req =~s/\((\d+)\s*\,\s*(\d+)\)/\($1\;$2\)/g;
	$req =~s/\s\((\d+)/\($1/g;
#	$req =~s/\)+/\)/g;
#
#	extract each declaration in the list
#
	my @reqdecs = split(',', $req);
	my $decl = '';
	my $usingstr = '';
	my $typecnt = 0;
	foreach $decl (@reqdecs) {
#
#	skip the name part, get the typestring
#
		if ($decl=~/\S+\s+(\S+)/) { 
			$decl = $1;
			$decl=~s/^\((.+)\)$/$1/;
			if ($decl=~/(\w+)\(([^\)]+)\)/) { 
				$decl = $1; 
				my $decsz = $2;
				if ($decsz=~/(\d+)\;(\d+)/) {
#
#	handle scaled decimal declarations
#
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
#
#	prepare a (possibly parameterized) statement
#
sub prepare {
#
#	Lotsa params here, lets define them:
#	$sessno	- session number of associated DBMS connection
#	$stmt	- text of the SQL statement
#	\@fname	- arrayref to recv names of returned columns
#	\@ftype	- arrayref to recv types of returned columns
#	\@fprec	- arrayref to recv precision/lengths of returned columns
#	\@fscale - arrayref to recv scales of returned DECIMAL columns
#	\@fnullable - arrayref to recv nullability of columns
#	\@ptypes - arrayref to recv types of SQL stmt parameters
#	\@plens - arrayref to recv lengths of SQL stmt parameters
#	\$usephs - ref to recv number of placeholders used
#	\@acttype - arrayref to recv type of each stmt in request/MACRO
#	\@actcount - arrayref to recv effected rows count of each stmt in request/MACRO
#	\@actwarns - arrayref to recv warnings for each stmt in request/MACRO
#	\@actstarts - arrayref to recv start index of each stmt's returned column
#		descriptions in the @fname, @fprec, and @fscale arrays
#	\@actends - arrayref to recv end index of each stmt's returned column
#		descriptions in the @fname, @fprec, and @fscale arrays
#	\@actsumstarts - arrayref to recv start index of each stmt's returned summary
#		column descriptions in the @fname, @fprec, and @fscale arrays
#	\@actsumends - arrayref to recv end index of each stmt's returned summary
#		column descriptions in the @fname, @fprec, and @fscale arrays
#	\@ftitle - arrayref to recv titles of returned columns
#	\@fformat - arrayref to recv formats of returned columns
#	$partition - the partition of the session
#
	my($sessno, $dbreq, $fname, $ftype, $fprec, $fscale, $fnullable, $ptypes, 
		$plens, $usephs, $acttype, $actcount, $actwarns,
		$actstarts, $actends, $actsumstarts, $actsumends, $ftitle, $fformat,
		$partition) = @_;
#
#	empty statements on non-SQL partitions are assumed
#	to indicate raw mode input applied to utilities
#	
	if (($partition eq 'FASTLOAD') && ($dbreq=~/^\s*;\s*$/)) {
		$$usephs = 0;
		$$ptypes[0] = DBI::SQL_VARBINARY;
		$$plens[0] = 32000;
		return 1;
	}
	if ($partition eq 'EXPORT') { 
		$$usephs = 0; 
		$$ptypes[0] = DBI::SQL_INTEGER;
		$$plens[0] = 4;
		$$ptypes[1] = DBI::SQL_INTEGER;
		$$plens[1] = 4;
		return 2;
	}
#
#	count the number of placeholders
#
	$lasterr{$sessno} = 0;
	$lastemsg{$sessno} = '';
	if ($dbreq=~/^\s*HELP\s*/i) {
		$lasterr{$sessno} = -1010;
		$lastemsg{$sessno} = 'Failure -1010: HELP statement not supported.';
		return undef;
	}
	my $tmpreq = $dbreq;
	$tmpreq=~s/\'\'//g;	# get rid of quoted quotes
	$tmpreq =~s/\'.+\'//g;	# get rid of quoted stuff

	my $phcnt = 0;
	my $x = 0;
	my $datainfo = '';
	$$usephs = ($tmpreq =~ tr/\?//);
	if ($$usephs !=  0) {
		if (($partition ne 'DBC/SQL') || defined($seslsn{$sessno})) {
			$lasterr{$sessno} = -1020;
			$lastemsg{$sessno} = 'Failure -1020: Placeholders not supported for utility applications.';
			return undef;
		}
#
#	OK, we've got placeholders, but we don't have any
#	way of getting the bound parameter types...so just
#	use VARCHAR(255) for everything and hope for the best
#
		$datainfo = pack('S', $$usephs);
		for (my $i = 0; $i < $$usephs; $i++) {
#
#	build a DATAINFO parcel for a nullable VARCHAR of default size
#
			$datainfo .= pack('SS', 449, $phdfltsz);
			$$ptypes[$i] = DBI::SQL_VARCHAR;
			$$plens[$i] = $phdfltsz;
		}
	}
#
#	now check for a USING clause...note that PH's and USING are *not*
#	compatible, so if we get both in the same stmt, then error out
#
	my $usingvars = parse_using($tmpreq, $ptypes, $plens);
	if ($usingvars == -1) {	# invalid USING clause
		$lasterr{$sessno} = 500;
		$lastemsg{$sessno} = "Failure 500: Invalid USING clause.";
		return undef; 
	}
	if (($$usephs > 0) && ($usingvars > 0)) {  # can't mix the two
		$lasterr{$sessno} = 501;
		$lastemsg{$sessno} = "Failure 501: Can't mix USING clause and placeholders in same statement.";
		return undef; 
	}
	
	my $reqlen = 4 + 10 + 4 + length($dbreq) + 6;
	if ($datainfo ne '') { $reqlen += length($datainfo) + 4; }
	my $reqmsg = buildtdhdr($COPKINDSTART, $reqlen, $sessno, 
		$sesauth{$sessno});
	if ($sesauth{$sessno} == 0xffffffff) { 
		$sesauthx{$sessno}++; 
		$sesauth{$sessno} = 0; 
	}
	else { $sesauth{$sessno}++; }

	if ($datainfo ne '') {
		$reqmsg .= pack("SSa10 SSa* SSA* SSS", 
			$PclOPTIONS,
			14,	# pclsize
			'RP',	# Request/execute mode

			$PclREQUEST,
			4 + length($dbreq),	# pclsize
			$dbreq,	# request

			$PclDATAINFO,
			4+length($datainfo),
			$datainfo,

			$PclRESP,
			6,	# pclsize
			$MaxRESPSz);	# max response size
	}
	else {
		$reqmsg .= pack("SSa10 SSA* SSS", 
			$PclOPTIONS,
			14,	# pclsize
			'RP',	# Request/execute mode

			$PclREQUEST,
			4 + length($dbreq),	# pclsize
			$dbreq,	# request

			$PclRESP,
			6,	# pclsize
			$MaxRESPSz);	# max response size
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

		if (($tdflavor == $PclENDSTATEMENT) ||
			($tdflavor == $PclENDREQUEST)) {
			$rspmsg = substr($rspmsg, $tdlen);
			next;
		}
		if ($tdflavor == $PclSUCCESS) {
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

		if (($tdflavor == $PclFAILURE) || ($tdflavor == $PclERROR)) {
#
#	extract/save error code msg
#
			($stmtno, $rowcnt, $tderr, $tdelen) = 
				unpack("SSSS", substr($rspmsg, 4));
			my $tdemsg = substr($rspmsg, 12, $tdelen);
			DBI->trace_msg("ERROR $tderr\: $tdemsg\n", 2);
			$lasterr{$sessno} = $tderr;
			$lastemsg{$sessno} = (($tdflavor == $PclFAILURE) ? "Failure" : "Error") .
				" $tderr\: $tdemsg on Statement $stmtno.";
				
			$sesstate{$sessno} = 0;	# we're idle now
			$sesinxact{$sessno} = 0;
			undef $$actsumstarts[$stmtno];
			undef $$actsumends[$stmtno];
			undef $$actstarts[$stmtno];
			undef $$actends[$stmtno];
			return undef;
		}

		if ($tdflavor !=  $PclPREPINFO) {
			$lasterr{$sessno} = $BADPARCEL;
			$lastemsg{$sessno} = 
				"Invalid parcel stream: got $tdflavor when PREPINFO expected.";
			return undef;
		}

		($tdflavor, $tdlen, $costest, $sumcnt, $colcnt) = 
			unpack("SSdSS", $rspmsg);
#
#	NOTE: we need to check for summary columns here...
#
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
#
#	associate column name w/ type
#
				$$fname[$nextcol] = $cname;
				if (($cnmlen == 0) && ($cttllen != 0)) {
					$$fname[$nextcol] = $ctitle;
				}
				else {
					$$fname[$nextcol] = $cname;
				}
				$$ftype[$nextcol] = $ptypemap{$datatype & $tdat_NULL_MASK};
				$$fnullable[$nextcol] = $datatype & 1;
				$$ftitle[$nextcol] = $ctitle;
#
#	format specifiers seem to be specified in a somewhat
#	capricious manner, so try to normalize here
#
				$cfmt = "\U$cfmt\E";
				if ($cfmt=~/^\-(\-+)(.*)/) {
					my $lcfmt = length($1) + 1;
					$cfmt = "\-($lcfmt)$2";
				}
				$$fformat[$nextcol] = $cfmt;
#
#	modify format spec for BYTE/VARBYTE
#
				if (($$ftype[$nextcol] == DBI::SQL_BINARY) &&
					($$ftype[$nextcol] == DBI::SQL_VARBINARY)) {
					$$fformat[$nextcol] = "6($$fprec[$nextcol])";
				}

				if ($$ftype[$nextcol] == DBI::SQL_DECIMAL) {
					$$fprec[$nextcol] = int($datalen/256);
					$$fscale[$nextcol] = int($datalen%256);
				}
				else { 
					$$fprec[$nextcol] = $datalen;
					$$fscale[$nextcol] = 0; 
				}
	
				if ($$ftype[$nextcol] != DBI::SQL_DECIMAL) {
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
#
#	skip the extra column count for summary columns
#
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
#
#	execute a previously prepared stmt, using current bound params
#
sub execute {
	my ($sessno, $stmt, $datainfo, $indicdata, $nowait, 
		$stmtinfo, $stmtno, $rawmode, $keepresp, $sth) = @_;
	$lasterr{$sessno} = 0;
	$lastemsg{$sessno} = '';
	my $reqmsg = '';
	my $modepcl = $PclINDICDATA;
	if (defined($rawmode) && ($rawmode eq 'RecordMode')) {
		$modepcl = $PclDATA;
	}
	if ($sespart{$sessno} eq 'FASTLOAD') {	# must be a fastload
		$reqmsg = $sesbuff{$sessno} . pack("SSS", $PclRESP, 6, 512);
		$sesbuff{$sessno} = '';
	}
	elsif ($sespart{$sessno} eq 'EXPORT') {	# must be an EXPORT
		$reqmsg = pack('SSa* SSS', $PclREQUEST, 12, substr($stmt, 0, 8), $PclRESP, 6, $MaxRESPSz);
	}
	elsif ($sespart{$sessno} eq 'MONITOR') {	# must be a MONITOR
		if ($indicdata ne '') {
			$reqmsg = pack("SSA* SSa* SSS", 
				$PclINDICREQ,
				4 + length($stmt),	# pclsize
				$stmt,	# request

				$modepcl,
				length($indicdata) + 4,
				$indicdata,

				(($keepresp) ? $PclKEEPRESP : $PclRESP),
				6,	# pclsize
				$MaxRESPSz);	# max response size	
		}
		else {
			$reqmsg = pack("SSA* SSS", 
				$PclINDICREQ,
				4 + length($stmt),	# pclsize
				$stmt,	# request

				(($keepresp) ? $PclKEEPRESP : $PclRESP),
				6,	# pclsize
				$MaxRESPSz);	# max response size
		}
	}
	elsif ($indicdata ne '') {
		if ($datainfo ne '') {
			$reqmsg = pack("SSa10 SSA* SSa* SSa* SSS", 
				$PclOPTIONS,
				14,	# pclsize
				'IE',	# Request/execute mode

				$PclINDICREQ,
				4 + length($stmt),	# pclsize
				$stmt,	# request

				$PclDATAINFO,
				length($datainfo) + 4,
				$datainfo,

				$modepcl,
				length($indicdata) + 4,
				$indicdata,

				(($keepresp) ? $PclKEEPRESP : $PclRESP),
				6,	# pclsize
				$MaxRESPSz);	# max response size
		}
		else {
			$reqmsg = pack("SSa10 SSA* SSa* SSS", 
				$PclOPTIONS,
				14,	# pclsize
				'IE',	# Request/execute mode

				$PclINDICREQ,
				4 + length($stmt),	# pclsize
				$stmt,	# request

				$modepcl,
				length($indicdata) + 4,
				$indicdata,

				(($keepresp) ? $PclKEEPRESP : $PclRESP),
				6,	# pclsize
				$MaxRESPSz);	# max response size	
		}
	}
	else {
		$reqmsg = pack("SSa10 SSA* SSS", 
			$PclOPTIONS,
			14,	# pclsize
			'IE',	# Request/execute mode

			$PclINDICREQ,
			4 + length($stmt),	# pclsize
			$stmt,	# request

			(($keepresp) ? $PclKEEPRESP : $PclRESP),
			6,	# pclsize
			$MaxRESPSz);	# max response size
	}
	my $reqlen = length($reqmsg);
	if ($debug) { DBI->trace_msg(pcldump($reqmsg), 2); }
	$reqmsg = buildtdhdr($COPKINDSTART, $reqlen, $sessno, $sesauth{$sessno})
		. $reqmsg;
	if ($sesauth{$sessno} == 0xffffffff) { 
		$sesauthx{$sessno}++; 
		$sesauth{$sessno} = 0; 
	}
	else { $sesauth{$sessno}++; }
	if ($debug) { DBI->trace_msg(hexdump('Exec Request', $reqmsg), 2); }
	tdsend($sesmap{$sessno}, $reqmsg, 0) || return undef;
	$sesstate{$sessno} = 1;	# we're in-progress now
#
#	in nowait mode, return immediately
#
	if ($nowait != 0) { return -1; }

	my $rspmsg = gettdresp($sesmap{$sessno}, $sessno);
#	$sesstate{$sessno} = 2;	# we're ready now
	if ($rspmsg eq '') { return undef; }
	my ($tdflavor, $tdlen) = unpack("SS", $rspmsg);
#
#	keep reading response data until a RECORD parcel is recv'd;
#	we keep track of warnings and activity info on a per-statment
#	basis
#
	my ($rowcnt, $tderr, $fldcount, $activity, $tdelen, $tdemsg);
	while ($tdflavor !=  $PclRECORD) {
		if ($tdflavor == $PclENDREQUEST) {
			$sesstate{$sessno} = 0;	# we're idle now
			undef $curresp{$sessno};
			return 0;	# if we get here, then no data-returning stmts included
		}
		if (($tdflavor == $PclFAILURE) || ($tdflavor == $PclERROR)) {
#
#	extract/save error code msg
#
			($$stmtno, $rowcnt, $tderr, $tdelen) = 
				unpack("SSSS", substr($rspmsg, 4));
			$tdemsg = substr($rspmsg, 12, $tdelen);
			DBI->trace_msg("ERROR $tderr\: $tdemsg\n", 2);
			$lasterr{$sessno} = $tderr;
			$lastemsg{$sessno} = (($tdflavor == $PclFAILURE) ? "Failure" : "Error") .
				" $tderr\: $tdemsg on Statement $$stmtno.";
#			print "execute: $lastemsg{$sessno}\n";
			$sesstate{$sessno} = 0;	# we're idle now
			$sesinxact{$sessno} = 0;
			return undef;
		}
		if ($tdflavor ==  $PclSUCCESS) {
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
		elsif ($tdflavor ==  $PclWITH) {
			my $stmthash = $$stmtinfo[$$stmtno];
			$$stmthash{'IsSummary'} = (unpack("S", substr($rspmsg, 4)) - 1);
			if (!defined($$stmthash{'SummaryPosition'})) {
				my @sumpos = ();
				my @sumstart = ();
				$$stmthash{'SummaryPosition'} = \@sumpos;
				$$stmthash{'SummaryPosStart'} = \@sumstart;
			}
		}
		elsif ($tdflavor ==  $PclENDWITH) {
			my $stmthash = $$stmtinfo[$$stmtno];
			undef $$stmthash{'IsSummary'};
		}
		elsif ($tdflavor ==  $PclPOSSTART) {
			my $stmthash = $$stmtinfo[$$stmtno];
			my $sumpos = $$stmthash{'SummaryPosition'};
			my $sumstart = $$stmthash{'SummaryPosStart'};
			push(@$sumstart, scalar(@$sumpos));
		}
		elsif ($tdflavor ==  $PclPOSITION) {
			my $stmthash = $$stmtinfo[$$stmtno];
			my $sumpos = $$stmthash{'SummaryPosition'};
			push(@$sumpos, (unpack("S", substr($rspmsg, 4))));
		}
		elsif ($tdflavor == $PclENDSTATEMENT) {
			my $stmthash = $$stmtinfo[$$stmtno];
			undef $$stmthash{'SummaryPosition'};
			undef $$stmthash{'SummaryPosStart'};
		}
		elsif (($tdflavor == $PclDATAINFO) && 
			($sespart{$sessno} eq 'MONITOR') && ($tdlen > 4)) {
			DBD::Teradata::st::ProcDataInfo($sth, 
				substr($rspmsg, 4, $tdlen - 4), $$stmtno);
		}
		elsif (($tdflavor != $PclDATAINFO) && 
			($tdflavor !=  $PclPOSEND)) {
			$lasterr{$sessno} = $BADPARCEL;
			$lastemsg{$sessno} = "Received bad parcel $tdflavor.";
			$sesstate{$sessno} = 0;	# we're idle now
			return undef;
		}
		$rspmsg = substr($rspmsg, $tdlen);
		if ($rspmsg eq '') {
#
#	not end of request yet, get next response chunk
#
			$rspmsg = tdcontinue($sessno, $nowait); 
			if (!defined($rspmsg)) { return ''; }
			if ($nowait != 0) { return -1; }
		}
		($tdflavor, $tdlen) = unpack("SS", $rspmsg);
	}

	$curresp{$sessno} = $rspmsg;
	return $rowcnt;
}
#
#	fetch a single row
#
sub fetch { # sessno
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
#
#	keep reading response data until a RECORD parcel is recv'd;
#	we keep track of warnings and activity info on a per-statment
#	basis
#
	my ($tdflavor, $tdlen) = unpack("SS", $rspmsg);
	my ($rowcnt, $tderr, $fldcount, $activity, $tdelen, $tdemsg);
	while ($tdflavor !=  $PclRECORD) {
		if ($tdflavor == $PclENDREQUEST) {
			$sesstate{$sessno} = 0;	# we're idle now
			undef $curresp{$sessno};
			return 0;	# if we get here, then no data-returning stmts included
		}
		if (($tdflavor == $PclFAILURE) || ($tdflavor == $PclERROR)) {
#
#	extract/save error code msg
#
			($stmtno, $rowcnt, $tderr, $tdelen) = 
				unpack("SSSS", substr($rspmsg, 4));
			$tdemsg = substr($rspmsg, 12, $tdelen);
			DBI->trace_msg(print "ERROR $tderr\: $tdemsg\n", 2);
			$lasterr{$sessno} = $tderr;
			$lastemsg{$sessno} = (($tdflavor == $PclFAILURE) ? "Failure" : "Error") .
				" $tderr\: $tdemsg on Statement $stmtno.";
			$sesstate{$sessno} = 0;	# we're idle now
			$$currstmt = $stmtno;
			$sesinxact{$sessno} = 0;
			return undef;
		}
		if ($tdflavor ==  $PclSUCCESS) {
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
		elsif ($tdflavor ==  $PclWITH) {
			my $stmthash = $$stmtinfo[$$currstmt];
			$$stmthash{'IsSummary'} = (unpack("S", substr($rspmsg, 4)) - 1);
			if (!defined($$stmthash{'SummaryPosition'})) {
				my @sumpos = ();
				my @sumstart = ();
				$$stmthash{'SummaryPosition'} = \@sumpos;
				$$stmthash{'SummaryPosStart'} = \@sumstart;
			}
		}
		elsif ($tdflavor ==  $PclENDWITH) {
			my $stmthash = $$stmtinfo[$$currstmt];
			undef $$stmthash{'IsSummary'};
		}
		elsif ($tdflavor ==  $PclPOSSTART) {
			my $stmthash = $$stmtinfo[$$stmtno];
			my $sumpos = $$stmthash{'SummaryPosition'};
			my $sumstart = $$stmthash{'SummaryPosStart'};
			push(@$sumstart, scalar(@$sumpos));
		}
		elsif ($tdflavor ==  $PclPOSITION) {
			my $stmthash = $$stmtinfo[$stmtno];
			my $sumpos = $$stmthash{'SummaryPosition'};
			push(@$sumpos, (unpack("S", substr($rspmsg, 4))));
		}
		elsif ($tdflavor ==  $PclENDSTATEMENT) {
			my $stmthash = $$stmtinfo[$stmtno];
			undef $$stmthash{'SummaryPosStart'};
			undef $$stmthash{'SummaryPosition'};
		}
		elsif (($tdflavor == $PclDATAINFO) && 
			($sespart{$sessno} eq 'MONITOR') && ($tdlen > 4)) {
			DBD::Teradata::st::ProcDataInfo($sth, 
				substr($rspmsg, 4, $tdlen - 4), $$currstmt);
		}
		elsif (($tdflavor !=  $PclDATAINFO) &&
			($tdflavor !=  $PclPOSEND)) {
			$lasterr{$sessno} = $BADPARCEL;
			$lastemsg{$sessno} = "Received bad parcel $tdflavor.";
			$sesstate{$sessno} = 0;	# we're idle now
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
		for ($i = 0; (($i < $maxlen) && ($rspmsg ne '') && ($tdflavor == $PclRECORD)); $i++) {
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
#
#	return last error code for session
#
	my ($sessno) = @_;
	return $lasterr{$sessno};
}

sub errstr {
#
#	return last error msg for session
#
	my ($sessno) = @_;
	return $lastemsg{$sessno};
}
#
#	close the current request
#
sub finish {
	my ($sessno) = @_;

	if ((!defined($sesstate{$sessno})) ||
		($sesstate{$sessno} == 0)) { return 1; }
#
#	send CANCEL in a CONTINUE msg
#
	my $reqmsg = buildtdhdr($COPKINDCONTINUE, 4, $sessno, $sesauth{$sessno});
	if ($sesauth{$sessno} == 0xffffffff) { 
		$sesauthx{$sessno}++; $sesauth{$sessno} = 0; 
	}
	else { $sesauth{$sessno}++; }

	$reqmsg .= pack("SS", $PclCANCEL, 4);
	
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
#
#	look for an ENDREQUEST in the response
#
sub pclwalk {
	my($buf) = @_;
	my ($flavor, $pcllen) = (0,0);
	while (length($buf) > 0) {
		($flavor, $pcllen) = unpack('SS', $buf);
		if ($flavor == $PclENDREQUEST) { return undef; }
		$buf = substr($buf, $pcllen);
	}
	return 1;
}

sub FirstAvailable {
#
#	NOTE: we can only handle less than 30 sessions here, since
#	select() uses bit vectors, and they can only hold 32 bits,
#	and we know that its likely that fileno's 0, 1, and 2 are
#	probably taken...in future, we may need to set the sockets
#	to nonblock mode (if possible), and manually poll them
#	(not pretty). Better still, we may multithread from here...
#
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
#
#	check for errors
#
	foreach $i (@$sesslist) {
		if (vec($eout, fileno($sesmap{$i}), 1) == 1) {
			return $i;
		}
	}
	return undef;
}

sub FirstAvailList {	# same as FirstAvailable, but returns list of all available
#
#	NOTE: we can only handle less than 30 sessions here, since
#	select() uses bit vectors, and they can only hold 32 bits,
#	and we know that its likely that fileno's 0, 1, and 2 are
#	probably taken...in future, we may need to set the sockets
#	to nonblock mode (if possible), and manually poll them
#	(not pretty). Better still, we may multithread from here...
#
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
#
#	check for errors
#
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
#
#	suck in whatever's waiting
#
	my $rspmsg = gettdresp($sesmap{$sessno}, $sessno);
	if ($rspmsg eq '') { return undef; }
	my ($tdflavor, $tdlen) = unpack("SS", $rspmsg);
#
#	keep reading response data until a RECORD parcel is recv'd;
#	we keep track of warnings and activity info on a per-statment
#	basis
#
	my ($rowcnt, $tderr, $fldcount, $activity, $tdelen, $tdemsg);
	while ($tdflavor !=  $PclRECORD) {
		if ($tdflavor == $PclENDREQUEST) {
			$sesstate{$sessno} = 0;	# we're idle now
			undef $curresp{$sessno};
			return 0;	# if we get here, then no data-returning stmts included
		}
		if (($tdflavor == $PclFAILURE) || ($tdflavor == $PclERROR)) {
#
#	extract/save error code msg
#
			($$stmtno, $rowcnt, $tderr, $tdelen) = 
				unpack("SSSS", substr($rspmsg, 4));
			$tdemsg = substr($rspmsg, 12, $tdelen);
			DBI->trace_msg("ERROR $tderr\: $tdemsg\n", 2);
			$lasterr{$sessno} = $tderr;
			$lastemsg{$sessno} = (($tdflavor == $PclFAILURE) ? "Failure" : "Error") .
				" $tderr\: $tdemsg on Statement $$stmtno.";
			$sesstate{$sessno} = 0;	# we're idle now
			$sesinxact{$sessno} = 0;
			return undef;
		}
		if ($tdflavor ==  $PclSUCCESS) {
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
		elsif ($tdflavor ==  $PclWITH) {
			my $stmthash = $$stmtinfo[$$stmtno];
			$$stmthash{'IsSummary'} = (unpack("S", substr($rspmsg, 4)) - 1);
		}
		elsif ($tdflavor ==  $PclENDWITH) {
			my $stmthash = $$stmtinfo[$$stmtno];
			undef $$stmthash{'IsSummary'};
		}
		elsif (($tdflavor ==  $PclPOSSTART) ||
			($tdflavor ==  $PclPOSITION) ||
			($tdflavor ==  $PclPOSEND)) {
#
#	we may eventually want to return these in the stmt hash;
#	for now, we'll ignore them
#
		}
#		elsif ($tdflavor ==  $PclECHO) {
#
#	need to handle this eventually
#
#		}
		elsif (($tdflavor != $PclENDSTATEMENT) &&
			($tdflavor != $PclDATAINFO)) {
			$lasterr{$sessno} = $BADPARCEL;
			$lastemsg{$sessno} = "Received bad parcel $tdflavor.";
			$sesstate{$sessno} = 0;	# we're idle now
			return undef;
		}
		$rspmsg = substr($rspmsg, $tdlen);
		if ($rspmsg eq '') {
#
#	not end of request yet, get next response chunk
#
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
		pack('SS', (($mode eq 'IndicatorMode') ? $PclINDICDATA : $PclDATA), (length($row)+4)) . 
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
1;

{
package DBD::Teradata;

use vars qw($VERSION $err $errstr $state $drh %connections);
$VERSION = "1.11";
$drh = undef;
%connections = ();
$err = 0;
$errstr = '';
$state = '00000';

sub driver {
#
#	if we've already been init'd, don't do it again
#
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
}

{
package DBD::Teradata::dr;

$DBD::Teradata::dr::imp_data_size = 0;

sub connect {
	my ($drh, $dsn, $user, $auth, $attr) = @_;
	my $host;
	my $port;
#
#	extract hostname and optional port from DSN
#
	my $partition = 'DBC/SQL';
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
#
#	check on attributes
#
	my $lsn = undef;
	if (defined($attr)) {
		foreach my $key (keys(%$attr)) {
			if ($key eq 'tdat_lsn') {
				$lsn = $$attr{'tdat_lsn'};
				if ($lsn=~/\D+/) {
					$DBD::Teradata::err = -1;
					$DBD::Teradata::errstr = 'Non-numeric LSN specified.';
					return undef;
				}
			}
			elsif ($key eq 'tdat_utility') {
				$partition = $$attr{'tdat_utility'};
				if (($partition ne 'DBC/SQL') && ($partition ne 'FASTLOAD') && 
					($partition ne 'EXPORT') && ($partition ne 'MONITOR')) {
					$DBD::Teradata::err = -1;
					$DBD::Teradata::errstr = 'Unsupported partition specified.';
					return undef;
				}
			}
		}
	}
#
#	now connect to the DBMS
#
	my $sessno = DBD::Teradata::impl::connect($host, $port, $user, $auth, undef, \$lsn, $partition,
		\$DBD::Teradata::err, \$DBD::Teradata::errstr);
	if (!defined($sessno)) {
		return undef;
	}
#
#	create a new connection handle for a connection
#
	my $dbh = DBI::_new_dbh($drh,{
		'Name' => $dsn,
		'USER' => $user,
		'CURRENT_USER' => $user
	});
#
#	store our useful info here, and make sure to 
#	reflect it back up
#
	if (defined($attr)) {
		foreach my $key (keys(%$attr)) {
			if ($key eq 'tdat_lsn') {
				$$attr{'tdat_lsn'} = $lsn;
			}
		}
	}
	$dbh->STORE('tdat_lsn', $lsn);
	$dbh->STORE('tdat_host', $host);
	$dbh->STORE('tdat_utility', $partition);
	$dbh->STORE('tdat_sessno', $sessno);
#	
#	save the handle for future reference
#
	$DBD::Teradata::connections{$sessno} = $dbh;
	$DBD::Teradata::impl::sesinxact{$sessno} = 0;
	$dbh;
}
#
#	sorry, no way to know...
#
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
#
#	wait for first idle session
#
sub FirstAvailable {
	my($drh, $dbhlist, $timeout) = @_;
	my $i = 0;
	my @sesslist;
#
#	default timeout is wait forever
#
	if (!defined($timeout)) { $timeout = -1; }
	my $dbh;
	foreach $dbh (@$dbhlist) {
#
#	note that we allow undefined entries in the input
#	handle array, to make life easier for the application
#
		if (!defined($dbh)) { next; }
		$sesslist[$i++] = $dbh->{'tdat_sessno'};
	}
	my $sessno = DBD::Teradata::impl::FirstAvailable(\@sesslist, $timeout);
	if (!defined($sessno)) { return undef; }
#
#	now lookup the handle for the returned session, then return
#	its index to caller
#
	for ($i = 0; $i < scalar(@$dbhlist); $i++) {
		if ((defined($$dbhlist[$i])) &&
			($sessno == $$dbhlist[$i]->{'tdat_sessno'})) {
			return $i;
		}
	}
#
#	hmmm...shouldn't get here, but just in case...
#
	return undef;
}
#
#	wait for first idle session
#
sub FirstAvailList {
	my($drh, $dbhlist, $timeout) = @_;
	my $i = 0;
	my @sesslist;
#
#	default timeout is wait forever
#
	if (!defined($timeout)) { $timeout = -1; }
	my $dbh;
	foreach $dbh (@$dbhlist) {
#
#	note that we allow undefined entries in the input
#	handle array, to make life easier for the application
#
		if (!defined($dbh)) { next; }
		$sesslist[$i++] = $dbh->{'tdat_sessno'};
	}
	my @outlist = DBD::Teradata::impl::FirstAvailList(\@sesslist, $timeout);
	if (!@outlist) { return undef; }
#
#	now lookup the handle for each returned session, and add it's index to the
#	list we return to caller
#
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
#
#	UtilityLogon connects several utility sessions using the input
#	user, password, partition, and LSN, and prepares statement handles
#	on each of them using the input statement and attributes
#
sub UtilityLogon {
	my ($drh, $host, $user, $pass, $numsess, $partition, $lsn, $dbhary, $sthary, $stmt, $attr) = @_;
	
	@$dbhary = ();
	for my $i (0..$numsess-1) {
		$$dbhary[$i] = DBI->connect("dbi:Teradata:$host", $user, $pass,
			{
				PrintError => 0,
				RaiseError => 0,
				AutoCommit => 0,
				tdat_lsn => $lsn,
				tdat_utility => $partition
			}
		);
	}
	
	@$sthary = ();
	for my $i (0..$numsess-1) {
		$$sthary[$i] = $$dbhary[$i]->prepare($stmt, $attr);
	}
	return $numsess;
}
1;
}

{
package DBD::Teradata::db;

$DBD::Teradata::db::imp_data_size = 0;
#
#	PREPARE a 'SELECT * FROM $table' and return the
#	table info
#
sub table_info {
#	my($dbh, $table) = @_;
#	DBD::Teradata::impl::prepare($dbh->{'tdat_sessno'}, 
#		'SELECT * FROM ' . $table . ' ;');
}

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
#
#	Teradata doesn't like newlines, tabs, etc., so replace them
#
	if (($dbh->{'tdat_utility'} eq 'DBC/SQL') || ($dbh->{'tdat_utility'} eq 'MONITOR')) {
		$stmt =~s/[\n\r\t]/ /g;
	}
#
#	setup to collect all the returned column info
#
	my @fname = ();		# fieldnames
	my @fname_uc = ();	# uppercase
	my @fname_lc = ();	# lowercase
	my @ftype = ();		# field type
	my @ftitle = ();		# field titles
	my @fformat = ();		# field formats
	my @fprec = ();		# field precision/length
	my @fscale = ();	# field scale (DECIMAL type only)
	my @fnullable = ();	# is field nullable
	my @ptypes = ();	# parameter types
	my @plens = ();		# parameter lengths
	my $usephs = 0;		# if using '?', set to number of params
	my @stmtinfo = ();	# stmtinfo of each stmt in request

	my @acttype = ();	# type of each stmt in request
	my @actcount = ();	# rows effected by each stmt in request
	my @actwarns = ();	# warnings for each stmt in request
	my @actstarts = ();	# start index of each SELECT stmt in output row
	my @actends = ();	# ending index of each SELECT stmt in output row
	my @actsumstarts = (); # start index of summary columns in output row
	my @actsumends = (); # end index of summary columns in output row
	my $issum = undef;	# indicates whether a summary row is fetched
	my $numparams = 0;
	
	if ($dbh->FETCH('tdat_utility') eq 'EXPORT') {
#
#	exports use binary statements, so don't prepare
#	check for an inherited statement handle, and copy its
#	attributes
#
		@stmtinfo = (
			undef, 
			{
				ActivityType => "Export",
				ActivityCount => 0,
				Warning => undef,
				StartsAt => undef,
				EndsAt => undef,
				IsSummary => undef,
				SummaryStarts => undef,
				SummaryEnds => undef
			},
			undef
		);
		if (defined($$attribs{'tdat_clone'})) {
			my $csth = $$attribs{'tdat_clone'};
			my $cstmtinfo = $csth->{'tdat_stmt_info'};
			my $cstmthash = $$cstmtinfo[1];
			
			$stmtinfo[1] = {
				ActivityType => "Export",
				ActivityCount => 0,
				Warning => undef,
				StartsAt => $$cstmthash{'StartsAt'},
				EndsAt => $$cstmthash{'EndsAt'},
				IsSummary => undef,
				SummaryStarts => undef,
				SummaryEnds => undef
			};

			@fname = @{($csth->{NAME})};
			@ftype = @{($csth->{TYPE})};
			@ftitle = @{($csth->{tdat_TITLE})};
			@fformat = @{($csth->{tdat_FORMAT})};
			@fprec = @{($csth->{PRECISION})};
			@fscale = @{($csth->{SCALE})};
			@fnullable = @{($csth->{NULLABLE})};
			@ptypes = (DBI::SQL_INTEGER, DBI::SQL_INTEGER);
			@plens = (4, 4);
			$numparams = 2;
		}
	}
	elsif ($stmt=~/^\s*(BEGIN|CHECKPOINT|END)\s+LOADING/i) {
#
#	bypass prepare on these, let execute deal with them
#
		@stmtinfo = (
			undef, 
			{
				ActivityType => "$1 Loading",
				ActivityCount => 0,
				Warning => undef,
				StartsAt => undef,
				EndsAt => undef,
				IsSummary => undef,
				SummaryStarts => undef,
				SummaryEnds => undef
			},
			undef
		);
	}
	elsif ($stmt=~/^\s*(BEGIN|END)\s+FASTEXPORT/i) {
#
#	bypass prepare on these, let execute deal with them
#
		@stmtinfo = (
			undef, 
			{
				ActivityType => "$1 Export",
				ActivityCount => 0,
				Warning => undef,
				StartsAt => undef,
				EndsAt => undef,
				IsSummary => undef,
				SummaryStarts => undef,
				SummaryEnds => undef
			},
			undef
		);
	}
	elsif ($dbh->FETCH('tdat_utility') eq 'MONITOR') {
#
#	bypass prepare on these, let execute deal with them
#
		@stmtinfo = (
			undef, 
			{
				ActivityType => "PMPC",
				ActivityCount => 0,
				Warning => undef,
				StartsAt => undef,
				EndsAt => undef,
				IsSummary => undef,
				SummaryStarts => undef,
				SummaryEnds => undef
			},
			undef
		);
#
#	provide some dummy parameter info
#
		$numparams = 16;
		$usephs = 1;
	}
	else {
		$numparams = DBD::Teradata::impl::prepare($sessno, $stmt, 
			\@fname, \@ftype, \@fprec, \@fscale, \@fnullable, \@ptypes, \@plens,
			\$usephs, \@acttype, \@actcount, \@actwarns,
			\@actstarts, \@actends, \@actsumstarts, \@actsumends, \@ftitle, \@fformat,
			$dbh->{'tdat_utility'});
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
	}
#
# 	save type of activity for each statement in request or macro,
#	and rows effected/retrieved
#
	my ($outer, $sth) = DBI::_new_sth($dbh, { Statement => $stmt });
#
#	if attributes supplied, verify and save them
#
	$sth->STORE('tdat_nowait', 0);
	if (defined($attribs)) {
		foreach $attr (keys(%$attribs)) {
			if ($attr eq 'tdat_clone') { next; }	# don't save inherited statement handle
			$sth->STORE($attr, $$attribs{$attr});
		}
	}
	$sth->STORE('tdat_sessno', $sessno);
	$sth->STORE('tdat_stmt_num' => 0);
	$sth->STORE('tdat_stmt_info' => \@stmtinfo);
	$sth->STORE('tdat_rows' => -1);
#
#	make sure utility sessions use same wait semantic as statement
#
	if ($dbh->FETCH('tdat_utility') ne 'DBC/SQL') {
		$dbh->STORE('tdat_nowait', $sth->FETCH('tdat_nowait'));
	}
#
#	save input parameter values, types, and lengths
#
	my @params = ();
	$sth->STORE('tdat_params' => \@params);
	$sth->STORE('tdat_ptypes' => \@ptypes);
	$sth->STORE('tdat_plens' => \@plens);
	$sth->STORE('tdat_usephs' => $usephs); # 0 => USING clause, else PH's
	$sth->STORE('NUM_OF_PARAMS' => $numparams);

	if (($dbh->FETCH('tdat_utility') eq 'MONITOR') &&
		(!defined($sth->FETCH('tdat_raw')))) {
		$sth->STORE('NUM_OF_FIELDS' => 255);
	}
	else {
		$sth->STORE('NUM_OF_FIELDS' => scalar(@fname));
	}
#
#	generate all upper and lower case field names (is this trip really neccesary ?)
#
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
#
#	remove the handle from the internal handle list
#
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
#
#	explicit transaction mode, so send ET;
#
	my $sessno = $dbh->FETCH('tdat_sessno');

	if (($dbh->FETCH('tdat_utility') eq 'FASTLOAD') &&
		(DBD::Teradata::impl::get_buf_len($sessno) > 0)) {
#
#	transfer accumulated buffer to DBMS
#
		my $stmtno = 0;
		my @stmtinfo = ( undef, { });
		$DBD::Teradata::impl::sesinxact{$sessno} = 0;
		my $rowcnt = DBD::Teradata::impl::execute( $sessno, undef, undef,
			undef, $dbh->FETCH('tdat_nowait'), \@stmtinfo, \$stmtno, 
			undef, undef, undef);
		if (!defined($rowcnt)) {
			$DBD::Teradata::err = DBD::Teradata::impl::err($sessno);
			$DBD::Teradata::errstr = DBD::Teradata::impl::errstr($sessno);
			return undef;
		}
		return 1;
	}

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
#
#	purge accumulated buffer
#
	my $sessno = $dbh->FETCH('tdat_sessno');
	if ($dbh->FETCH('tdat_utility') eq 'FASTLOAD') {
		DBD::Teradata::impl::clear_buf;
		$DBD::Teradata::impl::sesinxact{$sessno} = 0;
		return 1;
	}
#
#	explicit transaction mode, so send ABORT;
#
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
}

{
package DBD::Teradata::st;

$DBD::Teradata::st::imp_data_size = 0;

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
#
#	array params only valid on FASTLOAD
#
		my $sessno = $sth->FETCH('tdat_sessno');
		my $dbh = $DBD::Teradata::connections{$sessno};
		if ($dbh->FETCH('tdat_utility') ne 'FASTLOAD') {
			$DBD::Teradata::err = -1;
			$DBD::Teradata::errstr = 'BindParamArray() valid only for FASTLOAD sessions.';
			return undef;
		}
	}
#
#	default data type for placeholders is VARCHAR or default size
#
	my $type = DBI::SQL_VARCHAR;
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
		if (($type == DBI::SQL_VARCHAR) ||
			($type == DBI::SQL_LONGVARCHAR) ||
			($type == DBI::SQL_LONGVARBINARY) ||
			($type == DBI::SQL_VARBINARY)) {
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
#
#	what do I need maxlen for ???
#
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
	my $loading = ($dbh->FETCH('tdat_utility') eq 'FASTLOAD') ? 1 : 0;
#
#	if params provided directly, then force
#	VARCHAR(256) type
#
	if ((@bind_values) && ($usephs != 0)) {
		for (my $i = 0; $i < $numParam; $i++) {
			$$ptypes[$i] = DBI::SQL_VARCHAR;
			$$plens[$i] = $phdfltsz;
		}
	}
#
#	check for rawmode, in which case, we expect only
#	a single parameter binding, which is the raw binary
#	row buffer
#
	my $rawmode = $sth->FETCH('tdat_raw');
	if (($dbh->FETCH('tdat_utility') ne 'EXPORT') &&
		($numParam != 0) && (defined($rawmode))) { $numParam = 1; }
	if (defined($params) && (@$params > $numParam)) {
		$DBD::Teradata::err = -1;
		$DBD::Teradata::errstr = 'Too many parameters provided.';
		return undef;
	}
	if ((!defined($params)) && ($numParam != 0) && (!defined($dbh->FETCH('tdat_loading')))) {
		$DBD::Teradata::err = -1;
		$DBD::Teradata::errstr = 
			'No parameters provided for parameterized statement.';
		return undef;
	}
	my $stmtno = 0;
#
#	need to check for array or in/out params, and adjust as needed
#
	my $maxparmlen = 1;
	for (my $i = 0; $i < $numParam; $i++) {
		if ((ref $$params[$i] eq 'ARRAY') &&
			(scalar(@{$$params[$i]}) > $maxparmlen)) { 
				$maxparmlen = scalar(@{$$params[$i]}); 
		}
	}
#
#	handle EXPORT case
#
	if ($dbh->FETCH('tdat_utility') eq 'EXPORT') {
		my $stmt = '';
		for (my $i = 0; $i < 2; $i++) {
			if ($$ptypes[$i] != DBI::SQL_INTEGER) {
				$DBD::Teradata::err = -1;
				$DBD::Teradata::errstr = 
					'EXPORT session requires 2 non-NULL INTEGER parameters.';
				return undef;
			}
			my $p = $$params[$i];
			if (!defined($p)) {
				$DBD::Teradata::err = -1;
				$DBD::Teradata::errstr = 
					'EXPORT session requires 2 non-NULL INTEGER parameters.';
				return undef;
			}
			if (ref $p eq 'ARRAY') {
				$p = $$p[0];
				$DBD::Teradata::err = -1;
				$DBD::Teradata::errstr = 
					'Parameter arrays not supported for EXPORT sessions.';
				return undef;
			}
			if (ref $p eq 'SCALAR') {
				$p = $$p;
			}
			$stmt .= pack('L', $p);
		}

		my $rowcnt = DBD::Teradata::impl::execute( $sessno, 
			$stmt, '', '', $sth->FETCH('tdat_nowait'), 
			$sth->FETCH('tdat_stmt_info'),
			\$stmtno, undef, undef, $sth);

		if (!defined($rowcnt)) {
			$DBD::Teradata::err = DBD::Teradata::impl::err($sessno);
			$DBD::Teradata::errstr = DBD::Teradata::impl::errstr($sessno);
		}
		$sth->STORE('tdat_stmt_num' => $stmtno);
		return $rowcnt;
	}
#
#	check for fastload control BEGIN LOADING
#
	if (($dbh->FETCH('tdat_utility') eq 'DBC/SQL') &&
		defined($dbh->FETCH('tdat_lsn')) &&
		($sth->FETCH('Statement')=~/^\s*BEGIN\s+LOADING/i)) {

		my $rowcnt = DBD::Teradata::impl::execute( $sessno, 
			$sth->FETCH('Statement'), '', '', 0, 
			$sth->FETCH('tdat_stmt_info'),
			\$stmtno, undef, undef, $sth);

		if (!defined($rowcnt)) {
			$DBD::Teradata::err = DBD::Teradata::impl::err($sessno);
			$DBD::Teradata::errstr = DBD::Teradata::impl::errstr($sessno);
		}
		else {
			$dbh->STORE('tdat_loading', 1);
		}
		$sth->STORE('tdat_stmt_num' => $stmtno);
		return $rowcnt;
	}
	
	my $datainfo = '';
	my $indicdata = '';
	my $fldcnt = ($dbh->FETCH('tdat_utility') eq 'MONITOR') ? scalar(@$params) : $numParam;
	if (defined($params) && (@$params != 0)) {
		if ($usephs != 0) {
#
#	DATAINFO parcel only needed for statements with placeholders,
#	not for USING clauses
#
			my $ptypes = $sth->FETCH('tdat_ptypes');
			my $plens = $sth->FETCH('tdat_plens');
			my $i;
			for ($i = 0; $i < $fldcnt; $i++) {
				if ($$ptypes[$i] eq DBI::SQL_VARCHAR) {	 # VARCHAR
#
#	since we assumed max length 256, we need to increase here if
#	input param is actually bigger than 256 bytes
#
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
				elsif ($$ptypes[$i] eq DBI::SQL_VARBINARY) {	 # VARBYTE
					$datainfo .= pack('SS', $ptypecodes{DBI::SQL_VARBINARY}+1,
						2 + $$plens[$i]);
				}
				else {	 # everything else
					$datainfo .= pack('SS', $ptypecodes{$$ptypes[$i]}+1, 
						$$plens[$i]);
				}
			}
			$datainfo = pack('Sa*', $i, $datainfo);
		}
#
#	iterate if in param array mode
#
		for (my $k = 0; $k < $maxparmlen; $k++) {
			if (!defined($rawmode)) {
#
#	build INDICDATA parcel
#
				my @indicvec = DBD::Teradata::impl::initIndic($fldcnt);
				my $ptypes = $sth->FETCH('tdat_ptypes');
				my $plens = $sth->FETCH('tdat_plens');
				for (my $i = 0; $i < $fldcnt; $i++) {
#
#	adjust for param arrays
#
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
#
#	backfill NULLs
#
					if (!defined($p)) {
						DBD::Teradata::impl::setIndicator(\@indicvec, $i);
						if (($$ptypes[$i] eq DBI::SQL_VARCHAR) ||
							($$ptypes[$i] eq DBI::SQL_VARBINARY)) {
							$indicdata .= pack('S', 0);
						}
						elsif (($$ptypes[$i] eq DBI::SQL_CHAR) ||
							($$ptypes[$i] eq DBI::SQL_BINARY)) {
							$indicdata .= pack("A$$plens[$i]", '');
						}
						elsif ($$ptypes[$i] eq DBI::SQL_DECIMAL) {
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
						else {	 # everything else
							$indicdata .= pack($ppackstr{$$ptypes[$i]}, 0);
						}
						next;
					}
#
#	else load the data
#
					if (($$ptypes[$i] eq DBI::SQL_VARCHAR) ||
						($$ptypes[$i] eq DBI::SQL_VARBINARY)) {
						$indicdata .= pack('Sa*', length($p), $p);
					}
					elsif (($$ptypes[$i] eq DBI::SQL_CHAR) ||
						($$ptypes[$i] eq DBI::SQL_BINARY)) {
						$indicdata .= pack("A$$plens[$i]", $p);
					}
					elsif ($$ptypes[$i] eq DBI::SQL_DECIMAL) {
						$indicdata .= DBD::Teradata::impl::cvt_flt2dec($p, 
							int($$plens[$i]/256), int($$plens[$i]%256));
					}
					else {	 # everything else
						$indicdata .= pack($ppackstr{$$ptypes[$i]}, $p);
					}
				}	# end for
				$indicdata = DBD::Teradata::impl::cvtIndics(\@indicvec) . $indicdata;
				$rawmode = 'IndicatorMode';
			} # end if not raw mode
			else {
#
#	rawmode: trim the length prefix and newline suffix
#	adjust for param arrays
#
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
#
#	fastload uses deferred transfer
#
			if ($dbh->FETCH('tdat_utility') eq 'FASTLOAD') {
				if ((DBD::Teradata::impl::get_buf_len($sessno) + length($indicdata)) > 32000) {
					$DBD::Teradata::err = -1;
					if ($maxparmlen > 1) {
						$DBD::Teradata::errstr = "Message buffer overflow at row $k; reduce parameter array size(s), then resubmit.";
						$DBD::Teradata::impl::sesinxact{$sessno} = 0;
						DBD::Teradata::impl::clear_buf($sessno);
					}
					else {
						$DBD::Teradata::errstr = "Message buffer overflow; commit, then resubmit.";
					}
					return undef;
				}
				$DBD::Teradata::impl::sesinxact{$sessno} = 1;
				DBD::Teradata::impl::append_buf($sessno, $rawmode, $indicdata);
				next;
			}
		} # end for each param array element
	} # end if params

	if ($dbh->FETCH('tdat_utility') eq 'FASTLOAD') {
		$sth->STORE('tdat_stmt_num' => 1);
		return $maxparmlen;
	}
#
#	not fastload, just send it
#	in future we may perform multiple executions for param-array data
#
	if (($dbh->FETCH('tdat_utility') eq 'DBC/SQL') && (!$dbh->FETCH('AutoCommit')) && 
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
#
#	to fake out DBI, we need to backfill the extra row fields with undef
#
	my @row = (undef) x ($sth->{NUM_OF_FIELDS});
		
	if (defined($rawmode)) {
		if (defined($colary)) {
#
#	return NULL row for array-bound columns
#
			return $sth->_set_fbav(\@row);
		}
		if ($rawmode eq 'RecordMode') {
#
#	trim off the indicators
#
			$data = substr($data, $ibytes);
		}
#
#	add length prefix and newline suffix
#
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
			if (($$ftypes[$fpos] eq DBI::SQL_VARCHAR) || 
				($$ftypes[$fpos] eq DBI::SQL_VARBINARY) ||
				($$ftypes[$fpos] eq DBI::SQL_LONGVARBINARY)) {
				my $flen = unpack("S", $data); 
				$data = substr($data, 2);
				if (($flen != 0) && (!$ibit)) {
					$row[$fpos] = unpack("a$flen", $data);
					if (defined($sth->FETCH('ChopBlanks')) && 
						($sth->FETCH('ChopBlanks') != 0)) {
						$row[$fpos] =~ s/\s+$//;
					}
				}
				if (length($data) > $flen) {
					$data = substr($data, $flen);
				}
				else { $data = ''; }
			}
			elsif (($$ftypes[$fpos] eq DBI::SQL_CHAR) || 
				($$ftypes[$fpos] eq DBI::SQL_BINARY)) {
				if (!$ibit) {
					$row[$fpos] = unpack("a$$fprec[$fpos]", $data);
					if (defined($sth->FETCH('ChopBlanks')) && 
						($sth->FETCH('ChopBlanks') != 0)) {
						$row[$fpos] =~ s/\s+$//;
					}
				}
				if (length($data) > $$fprec[$fpos]) {
					$data = substr($data, $$fprec[$fpos]);
				}
				else { $data = ''; }
			}
			elsif ($$ftypes[$fpos] eq DBI::SQL_FLOAT) {
				if (!$ibit) {
					$row[$fpos] = unpack("d", $data);
				}
				$data = substr($data, 8);
			}
			elsif ($$ftypes[$fpos] eq DBI::SQL_DECIMAL) {
				if (!$ibit) {
					$row[$fpos] = DBD::Teradata::impl::cvt_dec2flt($data, 
						$$fprec[$fpos], $$fscale[$fpos]);
				}
				my $decsz = 8;
				if ($$fprec[$fpos] <= 2) {
					$decsz = 1;
				}
				elsif ($$fprec[$fpos] <=  4) {
					$decsz = 2;
				}
				elsif ($$fprec[$fpos] <=  9) {
					$decsz = 4;
				}
				$data = substr($data, $decsz);
			}
			elsif (($$ftypes[$fpos] eq DBI::SQL_INTEGER) ||
				($$ftypes[$fpos] eq DBI::SQL_DATE)) {
				if (!$ibit) {
					$row[$fpos] = unpack("l", $data);
				}
				$data = substr($data, 4);
			}
			elsif ($$ftypes[$fpos] eq DBI::SQL_SMALLINT) {
				if (!$ibit) {
					$row[$fpos] = unpack("s", $data);
				}
				$data = substr($data, 2);
			}
			elsif ($$ftypes[$fpos] eq DBI::SQL_TINYINT) {
				if (!$ibit) {
					$row[$fpos] = unpack("c", $data);
				}
				$data = substr($data, 1);
			}
			if (defined($$colary[$i])) {
				$ary = $$colary[$i];
				$$ary[$k] = $row[$fpos];
			}
		}
	}
#	$sth->STORE('NUM_OF_FIELDS' => $numflds);
	return $sth->_set_fbav(\@row);
}
*fetchrow_arrayref = \&fetch;

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

sub ProcDataInfo {	# processes DATAINFO parcels on the fly
	my ($sth, $pcl, $stmtno) = @_;
	my $flds = unpack('S', $pcl);
	$pcl = substr($pcl, 2);
	$flds *= 2;
	my $descr = "S$flds";
	$flds /= 2;
	my @diflds = unpack($descr, $pcl);

	my @fname = ();		# fieldnames
	my @fname_uc = ();	# uppercase
	my @fname_lc = ();	# lowercase
	my @ftype = ();		# field type
	my @ftitle = ();		# field titles
	my @fformat = ();		# field formats
	my @fprec = ();		# field precision/length
	my @fscale = ();	# field scale (DECIMAL type only)
	my @fnullable = ();	# is field nullable

	my $i = 0;	
	for ($i = 0; $i < $flds; $i++) {
		if (!defined($ptypemap{($diflds[($i * 2)] & $tdat_NULL_MASK)})) {
			last;
		}
		$ftype[$i] = $ptypemap{($diflds[($i * 2)] & $tdat_NULL_MASK)};
		$fnullable[$i] = ($diflds[($i * 2)] & 1);
		my $len = $diflds[(($i * 2)+1)];
		if ($ftype[$i] == DBI::SQL_DECIMAL) {
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
}
__END__

=head1 NAME

DBD::Teradata - a DBI driver for Teradata

=head1 SYNOPSIS

  use DBI;

  $dbh = DBI->connect('dbi:Teradata:hostname', 'user', 'password');

See L<DBI> for more information.

=head1 DESCRIPTION

Refer to the included tdatdbd.html, or 
http://home.earthlink.net/~darnold/tdatdbd.html for detailed information.

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

        http://home.earthlink.net/~darnold/tdatdbd.html

    Bug reports/Comments/suggestions/enhancement requests may be sent to

        darnold@earthlink.net

    Please see the following files for more information:
    	tdatdbd.html - the User's Guide
    	
=head2 *** CHANGE HISTORY

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

    As a user or maintainer of a local copy of DBD::Teradata, you need
    to be aware of the following addresses:

    The DBI mailing lists located at

        dbi-announce@fugue.com          for announcements
        dbi-dev@fugue.com               for developer/maintainer discussions
        dbi-users@fugue.com             for end user level discussion and help

    To subscribe or unsubscribe to each individual list you may use the
    WWW at

        http://www.fugue.com/dbi

    or email at the following addresses

        dbi-announce-request@fugue.com
        dbi-dev-request@fugue.com
        dbi-users-request@fugue.com

    with your request in the body of the message.
    
    The Teradata mailing list is managed and archived at
    
    	http://home.ease.lsoft.com/archives/tdata-l.html
    	
    You should subscribe to this list for Teradata-specific help and info.
=cut
