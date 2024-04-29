********************************************************************************
* Does retirement make people happy? Evidence from old-age workers in Germany
* Database construction 
* Author: William Fernandez
********************************************************************************

********************************************************************************
* Setup 
********************************************************************************
	clear 			 all
	set more 		 off
	set maxvar       20000
	
	 
	global			 root "C:\Users\wfern\OneDrive - Universidad del Pacífico\Hertie School of Governance\Master Thesis\Data\Working Datasets"
	
********************************************************************************
*Merging databases
********************************************************************************

	*1. RV_VSKT (non retired) with RV_RTBN_retired_2020 (retired) 
    use              "$root\RV_VSKT.dta", clear
	merge            1:1 rv_id using "$root\RV_RTBN_retired_2020.dta"
	drop 		     _merge
	
	*2. VSKT & RTBN with SOEP keys (pid)
	merge            1:1 rv_id using "$root\keys.dta"
	gen              no_soep_key=1 if _merge==1
	drop if 	     _merge==2
	drop 		     _merge 
	
    *3. VSKT & RTBN with SOEP Datasets
	*3.1 Bioedu 
	merge            1:1 pid using "$root\Bioedu.dta"
	gen              no_bioedu_info=1 if _merge==1
	drop if  	     _merge==2
	drop 			 _merge 
	
	*Saving before opening the panel datasets 
	tempfile         ind_base 
	save  			 `ind_base'
	
	
  /*3.2 Pgen: Person-related Status and Generated Vars.
	          Also partner identification and marital status.*
			  First panel dataset. Time var: syear*
	3.3 Pequiv: Satisfaction with life */
	use              "$root\pgen.dta", clear
	merge            1:1 pid syear using "$root\pequiv.dta"
	gen              no_pequiv_info=1 if _merge==1
	gen              no_pgen_info=1 if _merge==2
	drop 			 _merge 

	merge 			 m:1 pid using  `ind_base'
	drop if 		 _merge==1
	gen 			 no_soep_info=1 if _merge==2
	drop 			 _merge 
	

********************************************************************************
*Sample Selection 
********************************************************************************
	*Dropping people with no soep information 
	drop if          no_soep_info == 1  //1817 observations deleted who didn't find a match in the SOEP datasets 
	
	*Keeping only individuals aged 60-70
	keep if 		 d11101>=60 & d11101<=70 //123,433 panel observations deleted
	clonevar         age_years = d11101
	
	*Dropping EM-Rente and Sonstige Leistungen
	drop if 		 LEAT==1 | LEAT==88 // 482 obs deleted
	
	*Dropping missing gender
	drop if 		 GEVS==-2 //367 obs deleted
	
	*Missing SWL
	replace 		 p11101=. if p11101==-5 |  p11101==-2 |  p11101==-1	
	clonevar         overall_swl = p11101
	drop if 		 overall_swl ==. //412 observations deleted
	
	*Creating birthdate variable (month and year)
	gen 			 year_birth = GBJAVS
	replace 		 year_birth = gebjahr 

	replace          gebmonat=. if gebmonat==-1
	drop if 		 gebmonat==. //49 obs deleted
	
	gen 			 birth_date=ym(year_birth,gebmonat)
	format      	 birth_date %tm
	
	gen 			 survey_date=ym(syear, pgmonth)
	format 			 survey_date %tm
	gen 			 age_months=survey_date-birth_date
	
	
********************************************************************************
*Variable creation 
********************************************************************************   
	*Time periods before/after retirement
	replace 		 RTBEJ=. if RTBEJ==-2 | RTBEJ==0
	gen 			 years_retirement=syear-RTBEJ 
 
	
	*SRA cutoffs for different birth cohorts 
	gen 			 cutoff = 780 if GBJAVS<1947
	replace          cutoff = 781 if GBJAVS==1947
	replace          cutoff = 782 if GBJAVS==1948
	replace 		 cutoff = 783 if GBJAVS==1949
	replace          cutoff = 784 if GBJAVS==1950
	replace 		 cutoff = 785 if GBJAVS==1951
	replace          cutoff = 786 if GBJAVS==1952
	replace          cutoff = 787 if GBJAVS==1953
	replace          cutoff = 788 if GBJAVS==1954
	replace          cutoff = 789 if GBJAVS==1955
	replace          cutoff = 790 if GBJAVS==1956
	replace          cutoff = 791 if GBJAVS==1957
	replace          cutoff = 792 if GBJAVS==1958
	replace          cutoff = 794 if GBJAVS==1959
	replace          cutoff = 796 if GBJAVS==1960

	
    *Elegibility variable
	gen 			 Z1=age_months-cutoff // Z>0: eligible
	label var 		 Z1 "Age minus SRA (in months)"
	
	*Elegibility criteria
    gen	  		     eligibleZ1=1 if Z1>=0 
	replace 	     eligibleZ1=0 if Z1<0 
	
	gen 			 eligibility="Before SRA" if eligibleZ1==0
	replace 		 eligibility="After SRA" if eligibleZ1==1

	gen 	 	     interaction_EZ1=Z1*eligibleZ1

	*Retirement variable 
	gen 			 int_after_1st_pension = 1 if RTBEM < pgmonth & years_retirement==0
	replace			 int_after_1st_pension = 0 if RTBEM >= pgmonth & years_retirement==0
	
	gen 			 retired = 1 if years_retirement>0 
	replace			 retired = 0 if years_retirement<0 | years_retirement==.
	replace          retired = 1 if years_retirement==0 & int_after_1st_pension==1
	replace          retired = 0 if years_retirement==0 & int_after_1st_pension==0
	
	gen              retirement = "Employed" if retired==0
	replace  		 retirement = "Retired" if retired==1
	
	gen  			 aux= 1 if retired==1 & eligibleZ1==0
	replace          aux= 0 if retired==1 & eligibleZ1==1
	
	bys pid:         egen early_retirement = max(aux)   
	
	gen              retired_early = "Employed" if retired==0
	replace          retired_early = "Retired early" if retired==1 & early_retirement==1  
	replace          retired_early = "Retired after SRA" if retired==1 & early_retirement==0
	
	gen              retired_categories = "Employed" if retired==0
	replace          retired_categories = "Recently retired" if retired==1 & years_retirement<=2 & years_retirement!=. 
	replace          retired_categories  = ">2 years retired" if retired==1 & years_retirement>2 & years_retirement!=. 
	
	*Gender 
	gen 			 gender = "Female" if GEVS==2
	replace 		 gender = "Male" if GEVS==1 

	*Birth cohort 
	gen 			 birth_cohort = "1922-1940" if GBJAVS>=1922 & GBJAVS<=1940
	replace 		 birth_cohort = "1941-1945" if GBJAVS>=1941 & GBJAVS<=1945
	replace          birth_cohort = "1946-1950" if GBJAVS>=1946 & GBJAVS<=1950
	replace 		 birth_cohort = "1951-1960" if GBJAVS>=1951 & GBJAVS<=1960
	
	*Current self-rated health status
	gen				 self_health = "Very good or good" if m11126==1 | m11126==2
	replace          self_health = "Satisfactory" if m11126==3
	replace          self_health = "Poor or bad" if m11126==4 | m11126==5
	
	drop if 		 self_health == "" // 70 obs deleted

	*Education variable 
	gen              education_hs = "Less than HS" if d11108 == 1
	replace          education_hs = "High school" if d11108 == 2
	replace          education_hs = "More than HS" if d11108 == 3
	
	drop if 		 education_hs == "" // 8 obs deleted

	*Marital status 
	gen 			 married = "Married" if d11104==1
	replace          married = "Non-married" if d11104!=1 

	*State of Residence 
	gen 			 residence = "East Germany" if l11101_ew==22
	replace			 residence = "West Germany" if l11101_ew==21
	
	*Number of persons in hh
	clonevar         hh_members = d11106 

    *Overall SWL lag
	sort  			 pid age_years
	by pid:          gen overall_swl_lag = overall_swl[_n-1]
	
	*Keeping subsample to work in R
	keep			 pid syear RTBEJ RTBEM LEAT GEVS GBJAVS year_birth gebmonat gebjahr ///
					 years_retirement birth_date survey_date age_months gender birth_cohort ///				 
					 d11104 RTZB overall_swl age_years cutoff Z1 eligibleZ1 eligibility ///
					 interaction_EZ1 retired self_health education_hs married residence hh_members ///
					 retired_categories retired_early early_retirement retirement overall_swl_lag
			 
	
	save             "$root\Working_dataset_final.dta", replace 
	
	
	
	

	
	
	
	