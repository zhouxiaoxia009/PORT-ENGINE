package	Rimes::Portfolio::SGRAFI;
require Exporter;

@ISA=qw(Exporter);
@EXPORT_OK=qw(sgrafidate sgrafiport sgrafifile);
%EXPORT_TAGS=(ALL => \@EXPORT_OK);


use strict;

use Rimes::Global;
use Rimes::Core;
use Rimes::Ext::Utils;
use Rimes::Dates qw(:ALL);
use Rimes::User qw(:ALL);
use Rimes::Engine qw(:ALL);
use Rimes::Vendors::Gics qw(:ALL);
use Rimes::Holidays qw(:ALL);

sub	sgrafidate (;$$@) {

	my ($market, $date, @options)=@_;

	my %options = map { ($_, 1) } @options if(@options);
	my $corpact = $options{CORPACTION};
	my $nextday = $options{NEXTDAY};
	my $proforma = $options{PROFORMA};

	my $PUBHOME = $directories{SGRAFI}."\\";
    return if $nextday && $date eq "";

	$date = "" if ($date eq "LAST");

	return if $date !~ /^(\d{8})?$/;

	$date = longdate(busday(autodate($date))) if $date;

    my @dates;

    my @files;

	@files = grep {/\.tsv/i} listdir($PUBHOME."\\PORTS\\");
	if ($nextday) {
		@dates = map { $_ =~ /(\d{8})\.$market\.NEXTDAY\.tsv/i ? $1: () } @files;
	}elsif ($proforma){
		@dates = map { $_ =~ /(\d{8})\.$market\.PROFORMA\.tsv/i ? $1: () } @files;
	}elsif ($corpact){
		@dates = map { $_ =~ /(\d{8})\.$market\.CORPACT\.tsv/i ? $1: () } @files;
	}else{
		@dates = map { $_ =~ /(\d{8})\.$market\.CLOSE\.tsv/i ? $1: () } @files;
	}

	my	($mindate, $maxdate)= minmax @dates;

	if (!$date) {
		return $maxdate;
	}

	else{
		my @ltdates=grep { $_ <= $date } @dates if $date != 0;
		return @ltdates && $date <= $dates[-1] ? $ltdates[-1] : undef;
	}


}


sub	sgrafifile(;$$%) {

	my ($refdate, $market, %options)=@_;

	my $nextday = $options{NEXTDAY};
	my $proforma = $options{PROFORMA};
	my $corpact = $options{CORPACTION};

    my $filename;

	my $PUBHOME = $directories{SGRAFI}."\\";

	if ($nextday) {
		$filename = $PUBHOME."PORTS\\$refdate.$market.NEXTDAY.tsv";
	}elsif ($proforma){
		$filename = $PUBHOME."PORTS\\$refdate.$market.PROFORMA.tsv";
	}elsif ($corpact){
		$filename = $PUBHOME."PORTS\\$refdate.$market.CORPACT.tsv";
	}else{
		$filename = $PUBHOME."PORTS\\$refdate.$market.CLOSE.tsv";
	}

	#print "File is $filename\n";

	return -f $filename && $filename;
}



sub	sgrafiport($;$@) {

	my ($market, $date, @options)=@_;

	my %options = map { ($_, 1) } @options if(@options);
	my $nextday = $options{NEXTDAY};
	my $proforma = $options{PROFORMA};
	my $corpact = $options{CORPACTION};

	return unless useraccess("SGRAFIC");

	#Permissions per custom module/client
	if(useraccess("SGRAFICDPQ")){
		return unless $market =~ /^(RAMFEX|PFRAMFEX|RAVFEM|RALVFEM|RAQFEM|RAMFEM|RAMOMFEM)$/;
	}
	if(useraccess("SGRAFIPPA")){
		return unless $market =~ /^(PFRADMFX|PFRADMFE|PFRADMFU|RADMFX|RADMFE|RADMFU)$/;
	}
	if(useraccess("SGRAFIUBS")){
		return unless $market =~ /^(RAMFGEXW|PFRAMFGEXW)$/;
	}
	if(useraccess("SGRAFIUIG")){
		return unless $market =~ /^(RAFIFUNDUS)$/;
	}
	if(useraccess("SGRAFILGIM")){
		return unless $market =~ /^(RAQMFD)$/;
	}
	if(useraccess("SGRAFIRUSSIG")){
		return unless $market =~ /^(RAFIEM)$/;
	}

	my $refdate=sgrafidate($market, $date, @options) or return;

    my $filename=sgrafifile($refdate, $market, %options) or return;

	my $prevdate = get_prevdate($refdate, "NONE") or return;

	#print "date is $refdate, file is $filename\n";

    my $longdate=$refdate;

    my ($tmk, %tmk, @cmp);

	readtable($filename, \@cmp);

	my %openiv;

	my (%symbol_map, %symbol_map_r, %idstore);

	my @itemslist = qw(SYMBOL DESC BBTK SEDOL CUSIP); #items to chain


	if (!$nextday) {

		my %opt;
		$opt{NEXTDAY} = 1; #sgrafifile takes in a hash
		my $pnxtdate=sgrafidate($market, $prevdate, "NEXTDAY");
		my $openfilename=sgrafifile($pnxtdate, $market, %opt);

		#print "open filename = $openfilename \n";

		my @opencmp;

		readtable($openfilename, \@opencmp);

		if ($cmp[0]{DATE} eq $opencmp[0]{NEXTDATE}) { #strict date match, if faulty, we use TS anyway DL 20171106
			foreach my $record (@opencmp) {
				$openiv{$$record{SYMBOL}} ||= $$record{IVUSD}; #spincos with same symbol and 0 IVUSD do not overwrite parent IVUSD DL 20171106
			}
		}
	}else{

		######### CHAINTO LOGIC ###########

		my $CHAINTO = $directories{SGRAFI}."\\CHAINTO\\";

		%symbol_map = readinifile($CHAINTO."symbol_map.ini");

		my $nextdate = $cmp[0]{NEXTDATE};

		if($symbol_map{$nextdate}) {

			my $clsdate=sgrafidate($market, $nextdate, "");
			my $clsfilename=sgrafifile($clsdate, $market, "");

			my @clscmp;

			readtable($clsfilename, \@clscmp);

			%symbol_map = %{$symbol_map{$nextdate}};
			%symbol_map_r = reverse %symbol_map;

			foreach my $clsrecord (@clscmp) {
				if ($symbol_map_r{$$clsrecord{SYMBOL}}) {
					foreach my $item (@itemslist) {
						$idstore{$symbol_map_r{$$clsrecord{SYMBOL}}}{$item}=$$clsrecord{$item};
					}
				}
			}
		}

		###################################
	}

	#Calculates sum of market values
	foreach my $record (@cmp) {

		my $oldsymbol = $$record{SYMBOL}; #retain old symbol

		if ($idstore{$oldsymbol} and $nextday) { #fix with closing data and chainto mapping
			foreach my $item (@itemslist) {
				$$record{$item}=$idstore{$oldsymbol}{$item};
			}
		}

		$tmk{TOTALIVUSD} +=$$record{IVUSD};

		my $usedate = $nextday ? $$record{NEXTDATE} : $refdate;
		my $spinoffflag = autoserve("SGRAFI,$$record{SYMBOL},SPINOFFFLAG,$usedate,EXACT")||0;
		my $spinoffadj = autoserve("SGRAFI,$$record{SYMBOL},SPINOFFADJ,$usedate,EXACT")||1; #add a spinoffadj to deal with the case where price adjustment and zero price spinoff happen on the same time. This adj is reflecting the part of price adjustment


		if ($nextday) {

			my $div = autoserve("SGRAFI,$$record{SYMBOL},DIV,$$record{NEXTDATE},EXACT");
			my $niv = autoserve("SGRAFI,$$record{SYMBOL},NIV,$$record{NEXTDATE},EXACT");
			my $divccy = autoserve("SGRAFI,$$record{SYMBOL},DIV,$$record{NEXTDATE},CCY");
			my $nivccy = autoserve("SGRAFI,$$record{SYMBOL},NIV,$$record{NEXTDATE},CCY");
			my $fxdiv = autoserve("MSCI,$divccy,FX*USD,$$record{DATE}") || 1;
			my $fxniv = autoserve("MSCI,$nivccy,FX*USD,$$record{DATE}") || 1;
			my $prevup = autoserve("SGRAFI,$$record{SYMBOL},UP*USD,$$record{DATE},EXACT");

			#modified to not use time series for IVUSD; overcomes dual ADJ for spinoff and corpacts at the same time DL 20171106
			$$record{TRIVUSD} = $$record{IVUSD} - $$record{SHINV}*($div / $fxdiv) if $fxdiv;
			$$record{NRIVUSD} = $$record{IVUSD} - $$record{SHINV}*($niv / $fxniv) if $fxniv;

			$tmk{TOTALTRIVUSD} +=$$record{TRIVUSD};
			$tmk{TOTALNRIVUSD} +=$$record{NRIVUSD};

		}else {

			my $div = autoserve("SGRAFI,$$record{SYMBOL},DIV,$refdate,EXACT");
			my $niv = autoserve("SGRAFI,$$record{SYMBOL},NIV,$refdate,EXACT");
			my $divccy = autoserve("SGRAFI,$$record{SYMBOL},DIV,$refdate,CCY");
			my $nivccy = autoserve("SGRAFI,$$record{SYMBOL},NIV,$refdate,CCY");
			my $fxdiv = autoserve("MSCI,$divccy,FX*USD,$prevdate,EXACT") || 1;
			my $fxniv = autoserve("MSCI,$nivccy,FX*USD,$prevdate,EXACT") || 1;

			if (exists $openiv{$$record{SYMBOL}}) { #modified to not use time series for IVUSD; overcomes dual ADJ for spinoff and corpacts at the same time DL 20171106
				$$record{IIVUSD} = $openiv{$$record{SYMBOL}};
				$$record{ITRIVUSD} = $$record{IIVUSD} - $$record{SHINV}*($div / $fxdiv) if $fxdiv;
				$$record{INRIVUSD} = $$record{IIVUSD} - $$record{SHINV}*($niv / $fxniv) if $fxniv;
			}else{ #use time series way if symbol changes will still need M5 patch we can explore using DESC RIC BBTK TICKER later DL 20171106
				my $adj = ($spinoffflag) ? $spinoffadj : autoserve("SGRAFI,$$record{SYMBOL},ADJ,$$record{DATE},EXACT");
				$adj ||=1;
				my $prevup = autoserve("SGRAFI,$$record{SYMBOL},UP*USD,$prevdate,EXACT");
				$$record{IIVUSD} =  $$record{SHINV}*$prevup*$adj;
				$$record{ITRIVUSD} = $$record{SHINV}*($prevup*$adj - $div / $fxdiv) if $fxdiv;
				$$record{INRIVUSD} = $$record{SHINV}*($prevup*$adj - $niv / $fxniv) if $fxniv;
			}
		}

			$tmk{TOTALIIVUSD} +=$$record{IIVUSD};
			$tmk{TOTALITRIVUSD} +=$$record{ITRIVUSD};
			$tmk{TOTALINRIVUSD} +=$$record{INRIVUSD};


	}

	#Calculates IVLOC,UPUSD,IWGHT
	foreach my $record (@cmp) {
		$$record{IVLOC}||=$$record{UPLOC}*$$record{SHINV};
		$$record{UPUSD}||=$$record{IVUSD}/$$record{SHINV} if $$record{SHINV};
		$$record{IWGHT}=$$record{IVUSD}/$tmk{TOTALIVUSD} if $tmk{TOTALIVUSD}>0;
		$$record{TRIWGHT} = $$record{TRIVUSD}/$tmk{TOTALTRIVUSD} if ($nextday && $tmk{TOTALTRIVUSD}>0);
		$$record{NRIWGHT} = $$record{NRIVUSD}/$tmk{TOTALNRIVUSD} if ($nextday && $tmk{TOTALNRIVUSD}>0);
		$$record{IIWGHT}=$$record{IIVUSD}/$tmk{TOTALIIVUSD} if $tmk{TOTALIIVUSD}>0;
		$$record{ITRIWGHT} = $$record{ITRIVUSD}/$tmk{TOTALITRIVUSD} if $tmk{TOTALITRIVUSD}>0;
		$$record{INRIWGHT} = $$record{INRIVUSD}/$tmk{TOTALINRIVUSD} if $tmk{TOTALINRIVUSD}>0;
	}

	return @cmp;
}

1;
