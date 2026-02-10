#############################################################################################################
# lees_SQL_diverse_projectjes.R
#
#
# versie wie    wat
# 0.1    Niels  dbReadTable
# 0.2    Niels  extra querytjes voor BiBu, wegtype_SWOV
#               Alcohol (E24.08, ZonMW, HM)
#               Dieptestudie Motorongevallen (RD)
#############################################################################################################
#
#rm(list=ls())
.libPaths() # 

library(dplyr)
library(tidyr)
library(openxlsx)
library(readODS)
library(DBI)          # Database interface
library(data.table)
#require(lubridate)

# leesrechten op de wisdom DB in Uranus server
source("//hera/kiss/0_Management/AssignWisdom_viewer_Uranus_UTF8.R") # conUR

WisdomTabellen <- as.data.frame(odbc::dbListTables(conUR, catalog = 'Wisdom', schema = 'Admin1')) #497
names(WisdomTabellen) <- "tabelnaam"
  WisdomTabellen %>% filter(grepl("WEG", tabelnaam))

  dbListFields(conUR, "BRON_SLACHTOFFER")
  dbListFields(conUR, "BRON_OBJECT")
  dbListFields(conUR, "BRON_ONGEVAL")


#### Voorbeeld dbReadTable of dbGetQuery ####################################################################
  dbListFields(conUR, "BRON_ONGEVAL")
Ongevallen <- dbReadTable(conUR, "BRON_ONGEVAL") %>% # eerste helemaal binnenhalen
  filter(WEGBEH==1) %>%                             # en dan pas filteren
  filter(JAAR>2022)

  names(Ongevallen)
  table(Ongevallen[Ongevallen$OTE_A==94,]$VERVOERSW_SWOV_A, Ongevallen[Ongevallen$OTE_A==94,]$ERNONG5)

# Een sql query gaat sneller, en je kunt bepalen of de variabelen in hoofd- of kleine letters staan
vars <- paste0("jaar, ernong5, VORNUM, datum, niveaukop, wvk_idwegvak=wvk_id, x, y, wegnummer, hectometer, wegbeh, ",
               "wegsoort, WVK_ID=wvk_id1, loctypon, bst_code, bebouw, ote_A, ote_B, vervoersw_swov_A, vervoersw_swov_B")
qry <- paste0("SELECT ", vars, " FROM BRON_ONGEVAL WHERE JAAR>2007")
Ongevallen <- DBI::dbGetQuery(conn=conUR, qry)

  table(Ongevallen[Ongevallen$ote_A==94,]$vervoersw_swov_A, Ongevallen[Ongevallen$ote_A==94,]$ernong5)

#############################################################################################################


##### GEMEENTELIJKE HERINDELING (voorbeeld) #################################################################
  dbListFields(conUR, "GEMEENTEN_LIJST")

qry <- "SELECT var_nr, var_naam, cob_code AS code, cob_label as oms, cob_labelgb as omsgb, cob_omschrijving FROM codeboek
  WHERE var_naam IN ('gemeente_nr', 'provincie','politie_regio','arrondissement')
     OR var_nr IN (537, 25, 305, 1655)" # let op, soms is er wel een var_nr, maar is de var_naam leeg

codeboek <- DBI::dbGetQuery(conUR, qry)
count(codeboek, var_nr, var_naam)

(ref_provincie <- codeboek %>%
  filter(var_naam=="provincie" | var_nr==25) %>%
  rename(provincie=code, Provincie=oms) %>%
  mutate(provincie=as.integer(provincie),
    Provincie=case_when(
      Provincie=="FryslÃ¢n" ~ "Fryslân",
      TRUE ~ Provincie)) %>%
    arrange(provincie))

(ref_politie_regio <- codeboek %>%
    filter(var_naam=="politie_regio" | var_nr==305) %>%
    rename(politie_regio=code, Politieregio=oms) %>%
    mutate(politie_regio=as.integer(politie_regio),
           Politieregio=case_when(
             Politieregio=="FryslÃ¢n" ~ "Fryslân",
             TRUE ~ Politieregio),
           omsgb=case_when(
             omsgb=="FryslÃ¢n" ~ "Fryslân",
             TRUE ~ omsgb)) %>%
    arrange(politie_regio))

(ref_eenheid <- codeboek %>%
    filter(var_naam=="arrondissement" | var_nr==1655) %>%
    rename(eenheid=code, Eenheid=oms) %>%
    mutate(eenheid=as.integer(eenheid)) %>%
    arrange(eenheid))

 # ref_gemeente <- codeboek %>%
 #   filter(var_naam=="gemeente_nr" | var_nr==537) %>%
 #   rename(gemeente_nr=code, Gemeente_naam=oms) %>%
 #   mutate(gemeente_nr=as.integer(gemeente_nr))

Trans_gemeente <- dbReadTable(conUR, "GEMEENTEN_LIJST") %>% 
  select(gemeente_nr=GEMEENTE_NR, Gemeente_naam=GEMEENTE_NAAM, gemeente_nr_recent=GEMEENTEN_RECENT, provincie=PROVINCIE_ONBEKEND,
         politie_regio=POLITIE_REGIO, eenheid=ARRONDISSEMENT)

  names(Trans_gemeente)
  
Gemeente_recent <- Trans_gemeente %>%
  select(gemeente_nr_recent) %>% unique() %>% # 358
  left_join(Trans_gemeente %>% select(-gemeente_nr_recent), by = join_by(gemeente_nr_recent==gemeente_nr))

  names(Gemeente_recent)
  
Trans_gemeente <- Trans_gemeente %>% select(-provincie, -politie_regio, -eenheid) %>%
  left_join(Gemeente_recent %>% rename(Gemeente_naam_recent=Gemeente_naam), by = join_by(gemeente_nr_recent)) %>%
  left_join(ref_provincie %>% select(-var_nr, -var_naam, -omsgb, -cob_omschrijving), by = join_by(provincie)) %>%
  left_join(ref_politie_regio %>% select(-var_nr, -var_naam, -omsgb, -cob_omschrijving), by = join_by(politie_regio)) %>%
  left_join(ref_eenheid %>% select(-var_nr, -var_naam, -omsgb, -cob_omschrijving), by = join_by(eenheid))

  names(Trans_gemeente)
  unique(Trans_gemeente$Eenheid)
  unique(Trans_gemeente$Provincie)
  unique(Trans_gemeente$Politieregio)
  
#gerbuik:  left_join(Trans_gemeente, by = join_by(gemeente_nr)) %>%

#############################################################################################################

qry <- "SELECT cob_code AS code, cob_label as oms, cob_labelgb as omsgb, hoofdgroep, hoofdgroep_code FROM REF_BRON_VERVOERSW_SWOV"
(REF_BRON_VERVOERSW_SWOV <- DBI::dbGetQuery(conUR, qry) %>%
    arrange(hoofdgroep_code, code) %>%
    select(hoofdgroep, code, oms))

# Alcohol/art8
qry <- "SELECT var_nr, var_naam, cob_code AS code, cob_label as oms FROM codeboek WHERE var_nr=102"
(REF_alc <- DBI::dbGetQuery(conUR, qry))

dbListFields(conUR, "BRON_WEGCATEGORIE")

#per vornum is het wegtyps_SWOV aangegeven (obv wegbeh, wegnummer, bst_code, maxsnel en bebouw)
vars <- paste0("jaar, vornum, bst_code, wegbeheerder, wegsoort, wegtype_SWOV, wegcategorie, n_slacht_dood")
qry2 <- paste0("SELECT  ", vars, " FROM BRON_WEGCATEGORIE WHERE jaar >= 2020 AND ernong5=1")
wegcat   <- DBI::dbGetQuery(conn=conUR, qry2) %>%
  select(-wegcategorie, -wegsoort) %>%
  rename(baansoort=bst_code)

# Doden naar wegtype_SWOV
count(wegcat[wegcat$jaar>=2023,], wegtype_SWOV, wt=n_slacht_dood)
pivot_wider(count(wegcat[wegcat$jaar>=2020,], wegtype_SWOV, jaar, wt=n_slacht_dood), values_from=n, names_from=jaar)
# Dodelijke ongevallen
pivot_wider(count(wegcat[wegcat$jaar>=2020,], wegtype_SWOV, jaar), values_from=n, names_from=jaar)


vars <- paste0("jaar, vornum, bst_code, ernstsl, ote_sl, wegbeh, loctypon, bebouw, maxsne, vervoersw_swov")
qry <- paste0("SELECT ", vars, " FROM BRON_SLACHTOFFER WHERE jaar >2022 AND ernstsl < 6 ")

Slachtoffers <- DBI::dbGetQuery(conn=conUR, qry) %>%
  mutate(vornum=as.character(vornum)) %>%
  left_join(wegcat, by = join_by(jaar, vornum))

# hoeveel doden op bibeko50 naar Kruispunt/Wegvak?

table(Slachtoffers[Slachtoffers$jaar==2023 & Slachtoffers$maxsne==50,]$wegtype_SWOV)
pivot_wider(count(Slachtoffers[Slachtoffers$jaar==2023 & Slachtoffers$bebouw !=2,], maxsne, wegtype_SWOV, wegbeh), names_from=maxsne, values_from=n)


pivot_wider(count(Slachtoffers[Slachtoffers$jaar==2023 & Slachtoffers$wegtype_SWOV=="2 Binnen de bebouwde kom 50km/uur",], loctypon, wegbeh), names_from=wegbeh, values_from=n)

count(Slachtoffers[Slachtoffers$jaar==2024 & Slachtoffers$wegtype_SWOV=="2 Binnen de bebouwde kom 50km/uur",], loctypon)

Ongevallen <- Slachtoffers %>%
  filter(jaar==2023 & maxsne==50) %>% #wegtype_SWOV=="2 Binnen de bebouwde kom 50km/uur") %>%
  select(vornum, loctypon) %>%
  group_by(vornum, loctypon) %>%
  summarize(ndood=n(), .groups='drop') # 191

table(Ongevallen$loctypon)
count(Ongevallen, loctypon, wt=ndood) # 195


#### ZonMW Heike E24.08: ####
# slachtoffes naar vervoersw/tp/leeftijd/sexe per 168 uur
vars <- paste0("jaar, ernstsl, ote_sl, lftsl, sexesl, weekdag, uur, botspartner, vervoersw_swov")
qry <- paste0("SELECT ", vars, " FROM BRON_SLACHTOFFER WHERE jaar > 2020")

Slachtoffers <- DBI::dbGetQuery(conn=conUR, qry) %>%
  mutate(kleeft=case_when(
    between(lftsl, 0,15) ~  ' 0-15',
    between(lftsl, 16,17) ~ '16-17',
    between(lftsl, 18,24) ~ '18-24',
    between(lftsl, 25,34) ~ '25-34',
    between(lftsl, 35,49) ~ '35-49',
    between(lftsl, 50,120) ~'50+',
    TRUE ~ 'onbekend')) %>%
  mutate(tijd=case_when(
    weekdag==1 & between(uur, 6,17) ~ 'weekend dag',
    weekdag==7 & between(uur, 6,17) ~ 'weekend dag',
    between(uur,6,17) ~ 'week dag',
    weekdag==1 & between(uur, 0, 5) ~ 'weekendnacht',
    weekdag==6 & between(uur,18,24) ~ 'weekendnacht',
    weekdag==7 & between(uur, 0, 5) ~ 'weekendnacht',
    weekdag==7 & between(uur,18,24) ~ 'weekendnacht',
    TRUE ~ 'week nacht')) %>%
  mutate(vvm=case_when(
    ote_sl %in% c(71) ~ 'Voetganger',
    ote_sl %in% c(64,66,67) ~ 'Fietser',
    ote_sl %in% c(31,61,62) ~ 'GTW',
    ote_sl %in% c(1,11,21,22,23) ~ 'Motorvoertuig', # Personenwagen, Bestelwagen, Vrachtwagen
    ote_sl %in% c(93,94,32760, 32761) ~ 'Onbekend',
    ote_sl %in% c(65,41) ~ 'Anders',
    TRUE ~ 'Anders')) # mobility scooter, landbouwvoertuigen, …

table(Slachtoffers$lftsl, Slachtoffers$kleeft)
table(Slachtoffers$ote_sl, Slachtoffers$vvm)
#table(Slachtoffers$vervoersw_swov, Slachtoffers$vvm)
table(Slachtoffers$weekdag, Slachtoffers$tijd)
table(Slachtoffers$uur, Slachtoffers$tijd)
print(pivot_wider(count(Slachtoffers, weekdag, uur, tijd), names_from=uur, values_from=n), n=180)

Sla <- Slachtoffers %>%
  mutate(ernst=case_when(
    between(ernstsl,0,5) ~ 'Dood',
    between(ernstsl,6,8) ~ 'NaarZH',
    between(ernstsl,9,10) ~ 'Licht')) %>%
  mutate(geslacht=case_when(
    sexesl==1 ~ 'Man',
    sexesl==2 ~ 'Vrouw', 
    TRUE ~ 'Onbekend')) %>%
  mutate(tegenp=case_when(
    vvm %in% c('GTW','Motorvoertuig', 'Onbekend', 'Anders') ~ "nvt",
    botspartner %in% c(71) ~ 'voetganger',
    botspartner %in% c(64,66) ~ 'fietser',
    botspartner %in% c(1,11,21,22,23) ~ 'motorvoertuig',
    botspartner %in% c(32761) ~ 'geen',
    TRUE ~ 'anders')) %>%
  group_by(ernst, vvm, kleeft, geslacht, tijd, tegenp) %>%
  summarize(n=n(), .groups='drop')

# table(Sla[! Sla$vvm %in% c('Voetganger','Fietser'),]$botspartner, 
#       Sla[! Sla$vvm %in% c('Voetganger','Fietser'),]$tegenp)

write.csv2(Sla, file=file.path("//HERA/KISS/Qlik/ods/Projectjes","Sla_E2408.csv"), row.names=FALSE)

#### Motorongevallen RD #####################################################################################

dbListFields(conUR, "BRON_SLACHTOFFER")
vars <- paste0("jaar, vornum, ernstsl, ote_sl, wegbeh, loctypon, bebouw, maxsne, vervoersw_swov, botspartner, tegenpartij_swov, sexesl, lftsl, func, key_gem")
qry <- paste0("SELECT ", vars, " FROM BRON_SLACHTOFFER WHERE jaar >2021 AND jaar <2024")
Slachtoffers <- DBI::dbGetQuery(conn=conUR, qry) %>%
  mutate(vornum=as.character(vornum))

dbListFields(conUR, "BRON_ONGEVAL")
vars <- paste0("jaar, vornum, ernong5, datum, N_motor, wegbeh, loctypon, bebouw, maxsne, vervoersw_swov_A, vervoersw_swov_B, ernstbe1, ernstbe2, key_gem, gme_naam, pve_naam")
qry <- paste0("SELECT ", vars, " FROM BRON_ONGEVAL WHERE jaar >2021 AND jaar <2024 AND ernong5 <5")
Ongevallen <-  DBI::dbGetQuery(conn=conUR, qry) %>%
  mutate(vornum=as.character(vornum))
table(Ongevallen$vervoersw_swov_A, Ongevallen$N_motor)
Ongevallen <- Ongevallen %>%
  filter(N_motor>0 | vervoersw_swov_A %in% c(31,32,33) | vervoersw_swov_B %in% c(31,32,33) | vornum==20220126097) %>%
  filter(pve_naam=="Zuid-Holland") %>%
  filter(gme_naam %in% c("'s-Gravenhage","Delft","Leidschendam-Voorburg","Midden-Delfland","Pijnacker-Nootdorp","Rijswijk",
  "Wassenaar","Westland","Zoetermeer",
  "Alphen aan den Rijn","Bodegraven-Reeuwijk","Gouda","Hillegom","Kaag en Braassem","Katwijk","Krimpenerwaard","Leiden",
  "Leiderdorp","Lisse","Nieuwkoop","Noordwijk","Oegstgeest","Teylingen","Voorschoten","Waddinxveen","Zoeterwoude","Zuidplas"))
count(Ongevallen, gme_naam, key_gem)
unique(Ongevallen$gme_naam)#49
gemkeys <- unique(Ongevallen$key_gem)

Sla <- left_join(Slachtoffers, Ongevallen, by = join_by(jaar, vornum, wegbeh, loctypon, bebouw, maxsne, key_gem)) %>%
  filter(!is.na(ernong5) | ote_sl %in% c(31,32,33) | botspartner %in% c(31,32,33) | vervoersw_swov %in% c(31,32,33) | tegenpartij_swov %in% c(31,32,33)) %>%
  filter(key_gem %in% gemkeys) %>%
  mutate(ernst=case_when(
    ernstsl %in% c(0,1,2,3,4,5) ~ 'dood',
    ernstsl %in% c(6,7,8) ~ 'naar ZH',
    ernstsl %in% c(9,10) ~ 'licht')) %>%
  mutate(locatietype=case_when(
    loctypon==20 ~ 'Kruispunt',
    loctypon==21 ~ 'Wegvak')) %>%
  mutate(vvm=case_when(
    vervoersw_swov==31 ~ 'Motor',
    vervoersw_swov==32 ~ 'Motor 3wielen',
    ote_sl==31 ~ 'Motor',
    ote_sl==1 ~ 'Auto/bestel',
    ote_sl==11 ~ 'Auto/bestel',
    ote_sl==61 ~ 'Brom/snor',
    ote_sl==62 ~ 'Brom/snor',
    ote_sl==64 ~ 'Fiets/Ebike',
    ote_sl==65 ~ 'Scootm',
    ote_sl==66 ~ 'Fiets/Ebike',
    ote_sl==71 ~ 'Voetg',
    ote_sl==93 ~ 'GeenPartij',
    ote_sl==94 ~ 'Anders')) %>%
  mutate(tegenp=case_when(
    tegenpartij_swov==31 ~ 'Motor',
    tegenpartij_swov==32 ~ 'Motor 3wielen',
    botspartner==31 ~ 'Motor',
    botspartner==1 ~ 'Auto/bestel',
    botspartner==11 ~ 'Auto/bestel',
    botspartner %in% c(21,22,24,41) ~ 'VraBusLandb',
    botspartner==61 ~ 'Brom/snor',
    botspartner==62 ~ 'Brom/snor',
    botspartner==64 ~ 'Fiets/Ebike',
    botspartner==65 ~ 'Scootm',
    botspartner==66 ~ 'Fiets/Ebike',
    botspartner==71 ~ 'Voetg',
    botspartner %in% c(81,83,84,85,91) ~ 'Object/dier',
    botspartner==93 ~ 'GeenPartij',
    botspartner==94 ~ 'GeenPartij',
    botspartner==32761 ~ 'Geen')) %>%
  mutate(N_motor=ifelse(N_motor==0, 1, N_motor)) %>%
  mutate(wvd=case_when(
    vvm %in% c('Motor', 'Motor 3wielen') & func==1 ~ "bestuurder",
    vvm %in% c('Motor', 'Motor 3wielen') & func==2 ~ "passagier",
    TRUE ~ "nvt")) %>%
  select(-pve_naam, -key_gem, -ernstsl, -ernong5, -loctypon,
         -vervoersw_swov, -ote_sl, -tegenpartij_swov, -botspartner,
         -vervoersw_swov_A, -vervoersw_swov_B, -ernstbe1, -ernstbe2, -func)

unique(Sla$key_gem)
count(Sla, vvm, vervoersw_swov, ote_sl)
count(Sla, tegenp, tegenpartij_swov, botspartner)

write.csv2(Sla, file=file.path("//HERA/KISS/Qlik/ods/Projectjes","Sla_Motor_RD.csv"), row.names=FALSE)

####
