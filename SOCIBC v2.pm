package Rimes::Portfolio::SOCIBC;
require Exporter;

@ISA=qw(Exporter);
@EXPORT_OK=qw(socibcdate socibcport);
%EXPORT_TAGS=(ALL => \@EXPORT_OK);


use strict;

use Rimes::Core;
use Rimes::User;
use Rimes::Ext::Utils;
use Rimes::Dates qw(:ALL);
use Rimes::Global qw(:ALL);
use Rimes::Engine qw(:ALL);
use Rimes::Holidays qw(:ALL);

my %indices=(
	'CIBCMCE'	=>	{
		'CCY'	=>	'CAD',
		'HOLS'	=>	'CATSX',
	},
	'CIBCMUE'	=>	{
		'CCY'	=>	'USD',
		'HOLS'	=>	'USNYSE',
	},
);

sub socibcdate (;$$$) {
	my  ($market, $date, $options)=@_;

	$date=autodate($date, "yyyymmdd") if $date =~ /^\d{8}$/;

	my  @files=listdir("$directories{SOCIBC}\\PORTS");
	my  @dates;
	my  $type = ($options =~ /^(NEXTDAY)$/)? "OPEN":
				($options =~ /^(PROFORMA)$/)? "PROFORMA":"CLOSE";
	@dates = map { $_ =~ /(\d{8})\.$market.$type\.tsv/i ? ($1) : () } @files;


	my ($mindate, $maxdate)=minmax @dates;

	if ($date > 0 and ($date < $mindate or $date > $maxdate)) {
		return undef;

	} elsif ($date eq 'LAST') {
		if ($options =~ /^(NEXTDAY)$/){
			return $dates[-2];
		}else{
			return $maxdate;
		}

	} else {
		@dates=grep { $_ <= $date } @dates if $date != 0;
		return @dates ? $dates[-1] : undef;
	}

}

sub socibcfile(;$$$) {

	my  ($market, $refdate, $options)=@_;
	my  ($filename,$nifile,$gifile);
	my  $type = ($options =~ /^(NEXTDAY)$/)? "OPEN":
				($options =~ /^(PROFORMA)$/)? "PROFORMA":"CLOSE";

	if($options eq "NEXTDAY"){
		my $hols=$indices{$market}{HOLS};
		$refdate = get_nextdate($refdate,$hols);
		my  @files=listdir("$directories{SOCIBC}\\PORTS");
		my  @dates;
		@dates = map { $_ =~ /(\d{8})\.$market.$type\.tsv/i ? ($1) : () } @files;
		@dates=sort grep { $_ >=$refdate } @dates;
		$refdate=($refdate<$dates[0])?$dates[0]:$refdate;
	}

	$filename = "$directories{SOCIBC}\\PORTS\\$refdate.$market.$type.tsv";
	$nifile = "$directories{SOCIBC}\\PORTS\\$refdate.$market.NI.$type.tsv";
	$gifile = "$directories{SOCIBC}\\PORTS\\$refdate.$market.GI.$type.tsv";

	if(-f $filename){
		return($filename,$gifile,$nifile);
	}
}

sub socibcport($;$$) {

	my  ($market, $date, $options)=@_;
	#print "$market, $date, $options\n";
	$market = autoserve("SOCIBC,$market,SYMBOL");

	my  $type=($options=~/NEXTDAY/i)?'NEXTDAY':'CLOSE';

	my  $refdate=socibcdate($market, $date, $options)         or return;
	my  ($filename,$gifile,$nifile)=socibcfile($market, $refdate, $options)     or return;
	#print "$refdate $filename $gifile, $nifile\n";

	my (%trloc,%nrloc,%trivccy,%nrivccy,$baseccy,$tmk,$trtmk,$nrtmk,$itmk,$itrtmk,$inrtmk);
	$baseccy=$indices{$market}{CCY};

	if(-f $gifile){
		my @trrecs;
		readtable($gifile, \@trrecs);
		foreach my $rec (@trrecs) {
			$trloc{$$rec{SYMBOL}}=$$rec{TRLOC};
			$trivccy{$$rec{SYMBOL}}=$$rec{"IV".$baseccy};
		}
	}
	if(-f $nifile){
		my @trrecs;
		readtable($nifile, \@trrecs);
		foreach my $rec (@trrecs) {
			$nrloc{$$rec{SYMBOL}}=$$rec{NRLOC};
			$nrivccy{$$rec{SYMBOL}}=$$rec{"IV".$baseccy};
		}
	}

	my  @cmp;
    readtable($filename, \@cmp);

	#hedged portfolio
	if($market=~/_\w{3}$/)
	{
		return @cmp;
	}

	my $prevdate;

	my (%symbol_map, %symbol_map_r, %idstore);

	my @itemslist = qw(SYMBOL DESC BBTK SEDOL CUSIP); #items to chain

	if ($options =~ /NEXTDAY/i) {
		$prevdate = socibcdate($market, $date);

		######### CHAINTO LOGIC ###########

		my $CHAINTO = $directories{SOCIBC}."\\CHAINTO\\";

		%symbol_map = readinifile($CHAINTO."symbol_map.ini");

		my $nextdate = $cmp[0]{DATE};

		if($symbol_map{$nextdate}) {

			my $clsdate=socibcdate($market, $nextdate, "");
			my  ($clsfilename,$gifilename,$nifilename)=socibcfile($market, $clsdate, "")     or return;

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

	}else{
		$prevdate = socibcdate($market, longdate(busday(shortdate($date) - 1)));
		$openfilename=socibcfile($market, $prevdate, "NEXTDAY");  #"");?

		#print "open filename = $openfilename \n";

		my @opencmp;

		readtable($openfilename, \@opencmp);

		if ($cmp[0]{DATE} eq $opencmp[0]{NEXTDATE}) { #strict date match, if faulty, we use TS anyway DL 20171106
			foreach my $record (@opencmp) {
				$itmk{$$record{SYMBOL}} ||= $$record{"IV".$baseccy}; #spincos with same symbol and 0 IVUSD do not overwrite parent IVUSD DL 20171106
			}
		}
	}

    foreach my $record (@cmp) {

		my $oldsymbol = $$record{SYMBOL}; #retain old symbol

		if ($idstore{$oldsymbol} and $options =~ /NEXTDAY/i) { #fix with closing data and chainto mapping
			foreach my $item (@itemslist) {
				$$record{$item}=$idstore{$oldsymbol}{$item};
			}
		}

		$$record{MVLOC}=$$record{UPLOC}*$$record{SHOUT};
		$$record{IVLOC}=$$record{UPLOC}*$$record{SHINV};
		$$record{TRIVLOC}=$$record{UPLOC}*$$record{SHOUT};
		$$record{NRIVLOC}=$$record{UPLOC}*$$record{SHINV};
		$$record{"UP".$baseccy}=$$record{UPLOC}/$$record{FXRATE} if $$record{FXRATE};

		if ($options =~ /NEXTDAY/i) {
            $$record{NEXTDATE} = $$record{DATE};
            $$record{DATE} = $prevdate;
        }

		my $usedate = ($options =~ /^(NEXTDAY)$/) ? $$record{NEXTDATE} : $date;
		my $spinoffflag = autoserve("SOCIBC,$$record{SYMBOL},SPINOFFFLAG,$usedate,EXACT")||0;
		my $spinoffadj = autoserve("SOCIBC,$$record{SYMBOL},SPINOFFADJ,$usedate,EXACT")||1; #add a spinoffadj to deal with the case where price adjustment and zero price spinoff happen on the same time. This adj is reflecting the part of price adjustment


		if ($options =~ /NEXTDAY/i) {
        
			my $div = autoserve("SOCIBC,$$record{SYMBOL},DIV,$$record{NEXTDATE},EXACT");
			my $niv = autoserve("SOCIBC,$$record{SYMBOL},NIV,$$record{NEXTDATE},EXACT");
			my $divccy = autoserve("SOCIBC,$$record{SYMBOL},DIV,$$record{NEXTDATE},CCY");
			my $nivccy = autoserve("SOCIBC,$$record{SYMBOL},NIV,$$record{NEXTDATE},CCY");
			my $fxdiv = autoserve("MSCI,$divccy,FX*$baseccy,$$record{DATE}") || 1;
			my $fxniv = autoserve("MSCI,$nivccy,FX*$baseccy,$$record{DATE}") || 1;
			my $prevup = autoserve("SOCIBC,$$record{SYMBOL},UP*$baseccy,$$record{DATE},EXACT");

			#$$record{"IV".$baseccy} = $$record{SHINV}*$prevup*$adj;
			$$record{"TRIV".$baseccy} = $$$record{"IV".$baseccy} - $$record{SHINV}*($div / $fxdiv) if $fxdiv;
			$$record{"NRIV".$baseccy} = $$record{"IIV".$baseccy} - $$record{SHINV}*($niv / $fxniv) if $fxdiv;

			$trtmk +=$$record{"TRIV".$baseccy};
			$nrtmk +=$$record{"NRIV".$baseccy};
			$tmk +=$$record{"IV".$baseccy};

		}

		if ($type =~ /^(CLOSE)$/) {

			my $div = autoserve("SOCIBC,$$record{SYMBOL},DIV,$refdate,EXACT");
			my $niv = autoserve("SOCIBC,$$record{SYMBOL},NIV,$refdate,EXACT");
			my $divccy = autoserve("SOCIBC,$$record{SYMBOL},DIV,$refdate,CCY");
			my $nivccy = autoserve("SOCIBC,$$record{SYMBOL},NIV,$refdate,CCY");
			my $fxdiv = autoserve("MSCI,$divccy,FX*$baseccy,$prevdate,EXACT") || 1;
			my $fxniv = autoserve("MSCI,$nivccy,FX*$baseccy,$prevdate,EXACT") || 1;

			if (exists $itmk{$$record{SYMBOL}}) { #modified to not use time series for IVUSD; overcomes dual ADJ for spinoff and corpacts at the same time DL 20171106
				$$record{"IIV".$baseccy} = $itmk{$$record{SYMBOL}};
				$$record{"ITRIV".$baseccy} = $$record{"IIV".$baseccy} - $$record{SHINV}*($div / $fxdiv) if $fxdiv;
				$$record{"INRIV".$baseccy} = $$record{"IIV".$baseccy} - $$record{SHINV}*($niv / $fxniv) if $fxniv;
			}else{ #use time series way if symbol changes will still need M5 patch we can explore using DESC RIC BBTK TICKER later DL 20171106
				my $adj = ($spinoffflag) ? $spinoffadj : autoserve("SOCIBC,$$record{SYMBOL},ADJ,$$record{DATE},EXACT");
				$adj ||=1;
				my $prevup = autoserve("SOCIBC,$$record{SYMBOL},UP*$baseccy,$prevdate,EXACT");
				$$record{"IIV".$baseccy} =  $$record{SHINV}*$prevup*$adj;
				$$record{"ITRIV".$baseccy} = $$record{SHINV}*($prevup*$adj - $div / $fxdiv) if $fxdiv;
				$$record{"INRIV".$baseccy} = $$record{SHINV}*($prevup*$adj - $niv / $fxniv) if $fxniv;
			}
		}

			
			$itrtmk+=$$record{"ITRIV".$baseccy};
			$inrtmk+=$$record{"INRIV".$baseccy};
			$itmk+=$$record{"IIV".$baseccy};

		}

    }

    foreach my $record (@cmp) {
		$$record{IWGHT} = $$record{"IV".$baseccy}/$tmk if ($tmk>0);
		$$record{TRIWGHT} = $$record{"TRIV".$baseccy}/$trtmk if ($trtmk>0);
		$$record{NRIWGHT} = $$record{"NRIV".$baseccy}/$nrtmk if ($nrtmk>0);
		$$record{IIWGHT} = $$record{"IIV".$baseccy}/$itmk if ($itmk>0);
		$$record{ITRIWGHT} = $$record{"ITRIV".$baseccy}/$itrtmk if ($itrtmk>0);
		$$record{INRIWGHT} = $$record{"INRIV".$baseccy}/$inrtmk if ($inrtmk>0);
    }

    return @cmp;
}

1;