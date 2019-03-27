package Rimes::Portfolio::SOLCOR;
require Exporter;

@ISA=qw(Exporter);
@EXPORT_OK=qw(solcordate solcorport);
%EXPORT_TAGS=(ALL => \@EXPORT_OK);


use strict;

use Rimes::Core;
use Rimes::User;
use Rimes::Ext::Utils;
use Rimes::Dates qw(:ALL);
use Rimes::Global qw(:ALL);
use Rimes::Engine qw(:ALL);
use Rimes::Holidays qw(:ALL);
use Rimes::Engine::Encoding qw(get_source_encoding);

my %indexccy = (
    SFWUSP	=> 'USD',
    SFWGBP 	=> 'GBP',
    SFWDMUP	=> 'USD',
    SFWEBEP	=> 'EUR',
    SFWJPUP	=> 'USD',
    SFWPJUP	=> 'USD',
);

sub solcordate (;$$$) {
    my  ($market, $date, $options)=@_;

    $date=autodate($date, "yyyymmdd") if $date =~ /^\d{8}$/;

    my  @files=listdir("$directories{SOLCOR}\\PORTS");
    my  @dates;
    my  $type = ($options =~ /^(NEXTDAY)$/)? "OPEN":
                ($options =~ /^(PROFORMA)$/)? "PROFORMA":"CLOSE";
    @dates = map { $_ =~ /(\d{8})\.$market.$type\.tsv/i ? ($1) : () } @files;

    my ($mindate, $maxdate)=minmax @dates;


	my $nextdate = get_nextdate($date,"IXGLOBAL") if ($options eq "NEXTDAY" && $date ne 'LAST');

	################################################################################################################
	# Because nextday ports are named via open day data in the ports folder, we need to search on the get_nextdays
	# to search for the set of ports. Similarly, use get_prevday on the correct nextday port's file name to back into
	# the accepted NEXTDAY facet convention. This is important in that it impacts flags
	#
	# The solcorfile function will ultimately call get_nextdate to return the file that is we want to reference
	#
	# Ultimately, this is the con of using ports named via opening convention with NEXTDAY facets

	if ($date > 0 and ($date < $mindate or $date > $maxdate) and $options !~ /^(NEXTDAY)$/) {
        return undef;
    } elsif ($date > 0 and ($nextdate < $mindate or $nextdate > $maxdate) and $options =~ /^(NEXTDAY)$/) {
        return undef;
    } elsif ($date eq 'LAST') {
        if ($options =~ /^(NEXTDAY)$/){
            return $dates[-2];
        }else{
            return $maxdate;
        }
    } else {

        if ($options =~ /^(NEXTDAY)$/){
			@dates=grep { $_ <= $nextdate } @dates if $nextdate != 0;
			return @dates ? get_prevdate($dates[-1],"IXGLOBAL") : undef;
        } else {

			@dates=grep { $_ <= $date } @dates if $date != 0;
			return @dates ? $dates[-1] : undef;
		}
    }

}


sub solcorfile(;$$$) {
    my  ($market, $refdate, $options)=@_;
    my  $filename;
    my  $type = ($options =~ /^(NEXTDAY)$/)? "OPEN":
                ($options =~ /^(PROFORMA)$/)? "PROFORMA":"CLOSE";
    $refdate = autodate($refdate, "yyyymmdd") if ($refdate =~ /^\d+$/);

	$refdate = get_nextdate($refdate,"IXGLOBAL") if ($options eq "NEXTDAY");

    $filename = "$directories{SOLCOR}\\PORTS\\$refdate.$market.$type.tsv";

    return -f $filename && $filename;
}


sub solcorport($;$$) {
    my  ($market, $date, $options)=@_;

	my $baseccy;

	if ($indexccy{$market}) {
		$baseccy = $indexccy{$market};
	} else {
		$baseccy = "USD";
	}

    #return unless useraccess("SOLCORC");


    my  $refdate=solcordate($market, $date, $options)         or return;
    my  $filename=solcorfile($market, $refdate, $options)     or return;

    my  @cmp;
    readtable($filename, \@cmp);

    my ($tmv, $tiv, $tmk, %tmk);

	my $prevdate;

	my (%symbol_map, %symbol_map_r, %idstore);

	my @itemslist = qw(SYMBOL DESC BBTK SEDOL CUSIP); #items to chain

	if ($options =~ /NEXTDAY/i) {
		$prevdate = solcordate($market, $date);

		######### CHAINTO LOGIC ###########

		my $CHAINTO = $directories{SOLCOR}."\\CHAINTO\\";

		%symbol_map = readinifile($CHAINTO."symbol_map.ini");

		my $nextdate = $cmp[0]{DATE};

		if($symbol_map{$nextdate}) {

			my $clsdate=solcordate($market, $nextdate, "");
			my $clsfilename=solcorfile($market, $clsdate, "");

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
		 $prevdate = solcordate($market, longdate(busday(shortdate($date) - 1)));
	}

    foreach my $record (@cmp) {

		my $oldsymbol = $$record{SYMBOL}; #retain old symbol

		if ($idstore{$oldsymbol} and $options =~ /NEXTDAY/i) { #fix with closing data and chainto mapping
			foreach my $item (@itemslist) {
				$$record{$item}=$idstore{$oldsymbol}{$item};
			}
		}

		#dirty IV/MV fixes
		$$record{"MV".$baseccy} ||= $$record{MVLOC};
		$$record{"IV".$baseccy} ||= $$record{IVLOC};
		$$record{MVLOC} = $$record{UPLOC}*$$record{SHOUT};		
		$$record{IVLOC} = $$record{UPLOC}*$$record{SHINV};
		#dirty IV/MV fixes

        my $mv = $$record{MVLOC};
		$tmv += $mv;
		my $iv = $$record{IVLOC};
		$tiv += $iv;



		if ($options =~ /NEXTDAY/i) {
            $$record{NEXTDATE} = $$record{DATE};
            $$record{DATE} = $prevdate;
        }

		my $usedate = ($options =~ /^(NEXTDAY)$/) ? $$record{NEXTDATE} : $date;
		my $spinoffflag = autoserve("SOLCOR,$$record{SYMBOL},SPINOFFFLAG,$usedate,EXACT")||0;

		my $adj;
		if ($spinoffflag) {
			$adj = 1;
		}else{
			$adj = autoserve("SOLCOR,$$record{SYMBOL},ADJ,$$record{DATE},EXACT")||1;
			$adj = autoserve("SOLCOR,$$record{SYMBOL},ADJ,$$record{NEXTDATE},EXACT")||1 if ($options =~ /^(NEXTDAY)$/);
		}

		if ($options =~ /^(NEXTDAY)$/) {

			my $div = autoserve("SOLCOR,$$record{SYMBOL},DIV,$$record{NEXTDATE},EXACT");
			my $niv = autoserve("SOLCOR,$$record{SYMBOL},NIV,$$record{NEXTDATE},EXACT");
			my $divccy = autoserve("SOLCOR,$$record{SYMBOL},DIV,$$record{NEXTDATE},CCY");
			my $nivccy = autoserve("SOLCOR,$$record{SYMBOL},NIV,$$record{NEXTDATE},CCY");
			my $fxdiv = autoserve("MSCI,$divccy,FX*$baseccy,$$record{DATE}") || 1;
			my $fxniv = autoserve("MSCI,$nivccy,FX*$baseccy,$$record{DATE}") || 1;
			my $prevup = autoserve("SOLCOR,$$record{SYMBOL},UP*$baseccy,$$record{DATE},EXACT");

			$$record{"IV".$baseccy} = $$record{SHINV}*$prevup*$adj;
			$$record{"TRIV".$baseccy} = $$record{SHINV}*($prevup*$adj - $div / $fxdiv) if $fxdiv;
			$$record{"NRIV".$baseccy} = $$record{SHINV}*($prevup*$adj - $niv / $fxniv) if $fxniv;

			$tmk{"TOTALIV".$baseccy} +=$$record{"IV".$baseccy};
			$tmk{"TOTALTRIV".$baseccy} +=$$record{"TRIV".$baseccy};
			$tmk{"TOTALNRIV".$baseccy} +=$$record{"NRIV".$baseccy};

		}else {

			my $prevup = autoserve("SOLCOR,$$record{SYMBOL},UP*$baseccy,$prevdate,EXACT");
			my $currentup = autoserve("SOLCOR,$$record{SYMBOL},UP*$baseccy,$refdate,EXACT");
			my $div = autoserve("SOLCOR,$$record{SYMBOL},DIV,$refdate,EXACT");
			my $niv = autoserve("SOLCOR,$$record{SYMBOL},NIV,$refdate,EXACT");
			my $divccy = autoserve("SOLCOR,$$record{SYMBOL},DIV,$refdate,CCY");
			my $nivccy = autoserve("SOLCOR,$$record{SYMBOL},NIV,$refdate,CCY");
			my $fxdiv = autoserve("MSCI,$divccy,FX*$baseccy,$prevdate,EXACT") || 1;
			my $fxniv = autoserve("MSCI,$nivccy,FX*$baseccy,$prevdate,EXACT") || 1;

			$$record{"IV".$baseccy} =  $$record{SHINV}*$currentup;
			$$record{"IIV".$baseccy} =  $$record{SHINV}*$prevup*$adj;
			$$record{"ITRIV".$baseccy} = $$record{SHINV}*($prevup*$adj - $div / $fxdiv) if $fxdiv;
			$$record{"INRIV".$baseccy} = $$record{SHINV}*($prevup*$adj - $niv / $fxniv) if $fxniv;

			$tmk{"TOTALIV".$baseccy} +=$$record{"IV".$baseccy};
			$tmk{"TOTALIIV".$baseccy} +=$$record{"IIV".$baseccy};
			$tmk{"TOTALITRIV".$baseccy} +=$$record{"ITRIV".$baseccy};
			$tmk{"TOTALINRIV".$baseccy} +=$$record{"INRIV".$baseccy};

		}

    }

    foreach my $record (@cmp) {

        $$record{DESC} = $$record{DESC} || autoserve("SOLCOR,$$record{SYMBOL},DESC");
        $$record{IFACT} = $$record{SHINV}/$$record{SHOUT} if ($$record{SHOUT} > 0);
		$$record{IVW} = $$record{"IV".$baseccy} / $tmk{"TOTALIV".$baseccy} if $tmk{"TOTALIV".$baseccy}>0;
		$$record{IWGHT} = $$record{IVW};
		$$record{TRIWGHT} = $$record{"TRIV".$baseccy}/$tmk{"TOTALTRIV".$baseccy} if (($options =~ /^(NEXTDAY)$/) && $tmk{"TOTALTRIV".$baseccy}>0);
		$$record{NRIWGHT} = $$record{"NRIV".$baseccy}/$tmk{"TOTALNRIV".$baseccy} if (($options =~ /^(NEXTDAY)$/) && $tmk{"TOTALNRIV".$baseccy}>0);
		$$record{IIWGHT}=$$record{"IIV".$baseccy}/$tmk{"TOTALIIV".$baseccy} if $tmk{"TOTALIIV".$baseccy}>0;
		$$record{ITRIWGHT} = $$record{"ITRIV".$baseccy}/$tmk{"TOTALITRIV".$baseccy} if $tmk{"TOTALITRIV".$baseccy}>0;
		$$record{INRIWGHT} = $$record{"INRIV".$baseccy}/$tmk{"TOTALINRIV".$baseccy} if $tmk{"TOTALINRIV".$baseccy}>0;


    }
    return @cmp;
}

1;