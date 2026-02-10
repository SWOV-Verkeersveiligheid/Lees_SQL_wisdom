#############################################################################################################
# lees_SQL database.R
#
#
# versie wie    wanneer   wat
# 0.1    Niels  202502xx  dbReadTable
# 0.2    Niels  20250508  dbGetQuery, export van Verkeersdoden vanaf 1950
# 0.3    Niels  20250509  export Inwoners 1950 naar ods
# 0.4    Niels  20250510  IVO-tabellen (registratiegraad)
# 0.5    Niels  20260203  Ongevallen naar ernst, wegbeheerder en regio
#############################################################################################################
#
#rm(list=ls())
.libPaths() # 

#library(foreign)
library(dplyr)
library(tidyr)
library(openxlsx)
#library(writexl)
#install.packages('readODS')
library(readODS)
library(DBI)          # Database interface
library(data.table)
#require(lubridate)

# leesrechten op de wisdom DB in Uranus server
source("//hera/kiss/0_Management/AssignWisdom_viewer_Uranus_UTF8.R") # conUR

WisdomTabellen <- as.data.frame(odbc::dbListTables(conUR, catalog = 'Wisdom', schema = 'Admin1')) #497
names(WisdomTabellen) <- "tabelnaam"
  WisdomTabellen %>% filter(grepl("BRON", tabelnaam) & grepl("WEGT", tabelnaam))

  dbListFields(conUR, "BRON_SLACHTOFFER")
  dbListFields(conUR, "BRON_OBJECT")
  dbListFields(conUR, "BRON_ONGEVAL")
  dbListFields(conUR, "BRON_WEGTYPE")


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
#%>% rename(wvk_idwegvak=wvk_id, WVK_ID=wvk_id1)

  table(Ongevallen[Ongevallen$ote_A==94,]$vervoersw_swov_A, Ongevallen[Ongevallen$ote_A==94,]$ernong5)

vars <- paste0("jaar, vornum, ernong5, wegbeh, wegnummer, maxsne, bebouw, bst_code, wegsoort, loctypon, ote_sl, botspartner, vervoersw_swov, tegenpartij_swov, sexesl")
qry <- paste0("SELECT ", vars, " FROM BRON_SLACHTOFFER WHERE ERNSTSL < 6")
Slachtoffers <- DBI::dbGetQuery(conn=conUR, qry)
Wegtype <- dbReadTable(conUR, "BRON_WEGTYPE") #  helemaal binnenhalen
Wegtype <- Wegtype %>% mutate(vornum=as.numeric(KEY_ONG))

  Slachtoffers_wt <- left_join(Slachtoffers %>% mutate(vornum=as.numeric(vornum)),
                              Wegtype, by=join_by(vornum))
                              
  table(Slachtoffers[Slachtoffers$ote_sl==94,]$vervoersw_swov, Slachtoffers[Slachtoffers$ote_sl==94,]$ernong5)
  table(Slachtoffers_wt$wegtype, Slachtoffers_wt$jaar)
  count(Slachtoffers_wt, roadtype_INT, roadtype_BASELINE, roadtype_ITF, wegtype, wegtype_F)
  table(Slachtoffers_wt$roadtype_ITF, Slachtoffers_wt$jaar)
  pivot_wider(count(Slachtoffers_wt[Slachtoffers_wt$jaar>1995,], roadtype_ITF, sexesl, jaar), names_from=jaar, values_from=n)
  
#### Als QAP vervanging csv/ods maken voor relevante tabellen, incl codeboek ################################

##### VERKEERSDODEN VANAF 1950 ##############################################################################

  Wisdomtabellen %>% filter(grepl("DODEN", tabelnaam))
  dbListFields(conUR, "DODEN_LFT_WVD_TIJD")
  dbListFields(conUR, "codeboek")

# selecteer codeboeken
qry <- "SELECT var_nr, var_naam, cob_code AS code, cob_label as oms, cob_labelgb as omsgb, cob_omschrijving FROM codeboek
    WHERE var_naam IN ('leeftijd_4_75', 'vervoer2') OR var_nr IN (875,876)"

codeboek <- DBI::dbGetQuery(conUR, qry)
count(codeboek, var_nr, var_naam)
ref_leeftijd_4_75 <- codeboek %>%
  filter(var_naam=="leeftijd_4_75") %>%
  rename(LEEFTIJD_4_75=code, Leeftijdsklasse=oms) %>%
  mutate(LEEFTIJD_4_75=as.integer(LEEFTIJD_4_75))
ref_vervoer2 <- codeboek %>%
  filter(var_naam=="vervoer2") %>%
  rename(VERVOER2=code, Vervoerswijze=oms, TransportMode=omsgb) %>%
  mutate(VERVOER2=as.integer(VERVOER2))

# selecteer data  
Doden1950 <- dbReadTable(conUR, "DODEN_LFT_WVD_TIJD") %>%
  select(-ROWID) %>%
  filter(DOOD>0) %>%
  rename(Verkeersdoden=DOOD, Jaar=JAAR)
Doden1950_oms <- Doden1950  %>%
  left_join(ref_leeftijd_4_75, by = join_by(LEEFTIJD_4_75)) %>%
#  select(-var_nr, -var_naam) %>%
  left_join(ref_vervoer2, by = join_by(VERVOER2)) %>%
#  select(-var_nr, -var_naam) %>%
  mutate(Leeftijdsklasse=gsub("-"," t/m ",Leeftijdsklasse)) %>%
  mutate(Leeftijdsklasse=case_when(
    Leeftijdsklasse=="0  t/m  4" ~ " 0 t/m  4",
    Leeftijdsklasse=="5  t/m  9" ~ " 5 t/m  9",
    TRUE ~ Leeftijdsklasse)) %>%
  arrange(Jaar, Leeftijdsklasse, VERVOER2) %>%
  select(Jaar, Leeftijdsklasse, Vervoerswijze, TransportMode, Verkeersdoden)


count(Doden1950_oms, Leeftijdsklasse, wt=Verkeersdoden)
count(Doden1950_oms, Vervoerswijze, TransportMode, wt=Verkeersdoden)
count(Doden1950_oms, Jaar, wt=Verkeersdoden)
pivot_wider(count(Doden1950_oms[Doden1950_oms$Jaar==2024,], Leeftijdsklasse, Vervoerswijze, wt=Verkeersdoden), names_from=Vervoerswijze, values_from=n)


# schrijf data weg als csv/odt
write.csv2(Doden1950_oms, file=file.path("//HERA/KISS/Qlik/ods","Verkeersdoden_vanaf_1950.csv"), row.names=FALSE)
write_ods(Doden1950_oms, path=file.path("//HERA/KISS/Qlik/ods","Verkeersdoden_vanaf_1950.ods"), sheet = "Doden_1950")
          

##### BEVOLKING VANAF 1950 ##################################################################################

  WisdomTabellen %>% filter(grepl("INWON", tabelnaam))
  dbListFields(conUR, "INWONERS_CBS")
  #dbListFields(conUR, "INWONERS_CBS_VIEW")
  #dbListFields(conUR, "CBS_INWONERS_LEEFTIJD_GESLACHT")
  WisdomTabellen %>% filter(grepl("GEME", tabelnaam))
  dbListFields(conUR, "GEMEENTEN_LIJST")

qry <- "SELECT var_nr, var_naam, cob_code AS code, cob_label as oms, cob_labelgb as omsgb, cob_omschrijving FROM codeboek
  WHERE var_naam IN ('geslacht', 'gemeente_nr', 'provincie','politie_regio','arrondissement', 'wegbeh')
     OR var_nr IN (493, 537, 25, 305, 1655, 203)"

codeboek <- DBI::dbGetQuery(conUR, qry)
count(codeboek, var_nr, var_naam)
(ref_geslacht <- codeboek %>%
  filter(var_naam=="geslacht" | var_nr==493) %>%
  rename(geslacht=code, Geslacht=oms, Sex=omsgb) %>%
  mutate(geslacht=as.integer(geslacht)))

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

dbListFields(conUR, "VOR_LEEFTIJD")
Trans_leeftijd <- dbReadTable(conUR, "VOR_LEEFTIJD") %>% 
  select(lft_inw=LFTSL, Leeftijd=KLEEFT_NOVG) %>%
  filter(!is.na(lft_inw)) %>%
  select(-Leeftijd) %>%
  mutate(Leeftijd=case_when(
    between(lft_inw,  0,  5) ~ " 0- 5 jaar",
    between(lft_inw,  6, 11) ~ " 6-11 jaar",
    between(lft_inw, 12, 14) ~ "12-14 jaar",
    between(lft_inw, 15, 17) ~ "15-17 jaar",
    between(lft_inw, 18, 19) ~ "18-19 jaar",
    between(lft_inw, 20, 24) ~ "20-24 jaar",
    between(lft_inw, 25, 29) ~ "25-29 jaar",
    between(lft_inw, 30, 34) ~ "30-34 jaar",
    between(lft_inw, 35, 39) ~ "35-39 jaar",
    between(lft_inw, 40, 44) ~ "40-44 jaar",
    between(lft_inw, 45, 49) ~ "45-49 jaar",
    between(lft_inw, 50, 54) ~ "50-54 jaar",
    between(lft_inw, 55, 59) ~ "55-59 jaar",
    between(lft_inw, 60, 64) ~ "60-64 jaar",
    between(lft_inw, 65, 69) ~ "65-69 jaar",
    between(lft_inw, 70, 74) ~ "70-74 jaar",
    between(lft_inw, 75, 79) ~ "75-79 jaar",
    between(lft_inw, 80, 84) ~ "80-84 jaar",
    between(lft_inw, 85, 89) ~ "85-89 jaar",
    between(lft_inw, 90, 94) ~ "90-94 jaar",
    between(lft_inw, 95,120) ~ "95+",
    lft_inw==32760 ~ "Onbekend",
    TRUE ~ as.character(lft_inw))) 
#%>% mutate(Leeftijd=case_when(
    # between(lft_inw, 80, 84) ~ 8084,
    # between(lft_inw, 85, 89) ~ 8589,
    # between(lft_inw, 90, 94) ~ 9094,
    # between(lft_inw, 95,120) ~ 9599,
    # lft_inw==32760 ~ NA,
    # TRUE ~ Leeftijd))


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
  
  
Inwoners_cbs <- dbReadTable(conUR, "INWONERS_CBS") %>%
  select(-ROWID) # 13.083.021 (1950-2024)

  names(Inwoners_cbs)
  
Inwoners1950 <- Inwoners_cbs  %>%
  group_by(jaar, geslacht, lft_inw, gemeente_nr) %>%
  summarize(Inwoners=sum(aantal, na.rm=TRUE), .groups='drop') %>%
  left_join(ref_geslacht, by = join_by(geslacht)) %>%
  select(-var_nr, -var_naam, -geslacht) %>%
  left_join(Trans_leeftijd, by = join_by(lft_inw)) %>%
  left_join(Trans_gemeente, by = join_by(gemeente_nr)) %>%
  arrange(    jaar, provincie, eenheid, politie_regio, gemeente_nr, Gemeente_naam, gemeente_nr_recent, Gemeente_naam_recent, Geslacht, Sex, lft_inw) %>%
  select(Jaar=jaar, Provincie, Eenheid, Politieregio, gemeente_nr, Gemeente_naam, gemeente_nr_recent, Gemeente_naam_recent, Geslacht, Sex, Leeftijd, lft_inw, Inwoners) %>% 
  mutate(Provincie            = if_else(Jaar<1978 | is.na(Provincie), "Onbekend", Provincie),
         Eenheid              = if_else(Jaar<1978 | is.na(Eenheid) | gemeente_nr==997, "Onbekend", Eenheid),
         Politieregio         = if_else(Jaar<1978 | is.na(Politieregio), "Onbekend/nvt", Politieregio),
         gemeente_nr          = if_else(Jaar<1978, 9999, gemeente_nr),
         Gemeente_naam        = if_else(Jaar<1978, "Onbekend", Gemeente_naam),
         gemeente_nr_recent   = if_else(Jaar<1978, 9999, gemeente_nr_recent), 
         Gemeente_naam_recent = if_else(Jaar<1978, "Onbekend", Gemeente_naam_recent))

  #View(Inwoners1950[Inwoners1950$Jaar>1976 & Inwoners1950$lft_inw==26 & Inwoners1950$Geslacht=="Man",])
  #View(Inwoners1950[Inwoners1950$Jaar>1977 & Inwoners1950$lft_inw==40 & Inwoners1950$Geslacht=="Man" & Inwoners1950$gemeente_nr_recent<15,])
  table(Inwoners1950$Jaar) # eerst 192 records per jaar, vanaf 1978 154000 afnemend (500 naar 358 gemeenten)
  table(Inwoners1950$Jaar, Inwoners1950$Provincie) # arrange maakt niet uit voor table, die doet alpha
  #pivot_wider(count(Inwoners1950[Inwoners1950$Jaar>1976,], Jaar, provincie, wt=Inwoners), names_from=Jaar, values_from=n)
  pivot_wider(count(Inwoners1950[Inwoners1950$Jaar>1976,], Jaar, Leeftijd, wt=Inwoners), names_from=Jaar, values_from=n)
  
#laat lft_inw weg, dan ipv 3M records nog 674892 over -> past
Inwoners1950_recent_lft <- Inwoners1950  %>%
  group_by(Jaar, Geslacht, Sex, Leeftijd, gemeente_nr_recent, Gemeente_naam_recent, Provincie, Politieregio, Eenheid) %>%
  summarize(Inwoners=sum(Inwoners), .groups='drop') %>% # 3079524
  arrange(Jaar, Provincie, Eenheid, Politieregio, gemeente_nr_recent, Gemeente_naam_recent, Geslacht, Sex) %>%
  select( Jaar, Provincie, Eenheid, Politieregio, gemeente_nr_recent, Gemeente_naam_recent, Geslacht, Sex, Leeftijd, Inwoners)

  table(Inwoners1950_recent_lft$Jaar) # eerst 192 records per jaar, vanaf 1978 65000 = 100 * 2 * 358
  table(Inwoners1950_recent_lft$Jaar, Inwoners1950_recent_lft$Provincie)
  table(Inwoners1950_recent_lft$Jaar, Inwoners1950_recent_lft$Leeftijd)
  
losseleeftijden <- 0
if (losseleeftijden==1) {
  (series <- count(Inwoners1950_recent, Jaar) %>% # eerst 192 records per jaar
      arrange(-Jaar) %>%
      mutate(cum=cumsum(n)) %>%
      mutate(serie=case_when(
        cum < 1048575 ~ 1,
        cum < 983867 + 1048575 ~ 2,
        Jaar > 1977 ~ 3,
        TRUE ~ 4)))
  (series2 <- series %>% group_by(serie) %>%
      summarize(bestandsgrootte=sum(n), 
                minJ=min(Jaar),
                maxJ=max(Jaar),
                .groups='drop') %>%
      right_join(series, by = join_by(serie)))
  
  Inwoners_1950_recent <-  left_join(Inwoners1950_recent, series2 %>% select(-n, -cum, -bestandsgrootte), by = join_by(Jaar)) %>%
    select(-minJ, -maxJ)
  
  count(Inwoners1950, Gemeente_naam_recent, wt=Inwoners)
  print(pivot_wider(count(Inwoners_1950_recent, Jaar, Provincie, wt=Inwoners), names_from=Provincie, values_from=n), n=10000)
  print(pivot_wider(count(Inwoners_1950_recent, Jaar, Eenheid, wt=Inwoners), names_from=Eenheid, values_from=n), n=10000)
  
  # write.csv2(Inwoners_1950_recent %>% select(-serie), file=file.path("//HERA/KISS/Qlik/ods","Inwoners_gemeente_recent.csv"), row.names=FALSE)
  
  
  # #Schrijf naar ods
  # #NIEUW bestand
  # write_ods(Inwoners_gemeente_historisch,
  #           path=file.path("//HERA/KISS/Qlik/ods","Inwoners_gemeente.ods"),
  #           sheet = "Gemeente_historisch")
  # 
  # # AANVULLEN IN BESTAAND BESTAND
  # write_ods(Inwoners_1950_recent %>% filter(serie==4) %>% select(-serie, -Provincie,-Eenheid, -Politieregio, -gemeente_nr_recent, -Gemeente_naam_recent),
  #           path  = file.path("//HERA/KISS/Qlik/ods","Inwoners_gemeente.ods"),
  #           sheet = "Inwoners_1950_1977", append = TRUE)
  # write_ods(Inwoners_1950_recent %>% filter(serie==3) %>% select(-serie),
  #           path  = file.path("//HERA/KISS/Qlik/ods","Inwoners_gemeente.ods"),
  #           sheet = "Inwoners_1978_1993", append = TRUE)
  # write_ods(Inwoners_1950_recent %>% filter(serie==2) %>% select(-serie),
  #           path  = file.path("//HERA/KISS/Qlik/ods","Inwoners_gemeente.ods"),
  #           sheet = "Inwoners_1994_2009", append = TRUE)
  # write_ods(Inwoners_1950_recent %>% filter(serie==1) %>% select(-serie),
  #           path  = file.path("//HERA/KISS/Qlik/ods","Inwoners_gemeente.ods"),
  #           sheet = "Inwoners_2010_2024", append = TRUE)
  # #Fatal error abort bij 4e sheet >40GB?
  
  #Schrijf naar excel, sla die dan later op als ods
  # writexl::write_xls kan niet met meerdere sheets
  wb <- NULL
  #options("openxlsx.numFmt" = "#,##0") # 1000-tal scheiding, maar zonder decimalen
  # afhankelijk van je regional setings maakt hij er dan 123.456,789 van (dus 123.457)
  
  wb <- openxlsx::createWorkbook("Inwoners_gemeente")
  s <- createStyle(numFmt = "#,##0") # 1000-tal scheiding, maar zonder decimalen
  openxlsx::addWorksheet(wb, "Gemeente_historisch")
  openxlsx::writeData(wb,    "Gemeente_historisch", Inwoners_gemeente_historisch)
  openxlsx::addWorksheet(wb, "Inwoners_1950_1977")
  openxlsx::writeData(wb,    "Inwoners_1950_1977", Inwoners_1950_recent %>% filter(serie==4) %>% select(-serie, -Provincie,-Eenheid, -Politieregio, -gemeente_nr_recent, -Gemeente_naam_recent))
  openxlsx::addWorksheet(wb, "Inwoners_1978_1993")
  openxlsx::writeData(wb,    "Inwoners_1978_1993", Inwoners_1950_recent %>% filter(serie==3) %>% select(-serie))
  openxlsx::addWorksheet(wb, "Inwoners_1994_2009")
  openxlsx::writeData(wb,    "Inwoners_1994_2009", Inwoners_1950_recent %>% filter(serie==2) %>% select(-serie))
  openxlsx::addWorksheet(wb, "Inwoners_2010_2024")
  openxlsx::writeData(wb,    "Inwoners_2010_2024", Inwoners_1950_recent %>% filter(serie==1) %>% select(-serie))
  openxlsx::addStyle(wb,     "Gemeente_historisch", style= s, rows=2:nrow(Inwoners_gemeente_historisch), cols=9, gridExpand = TRUE)
  openxlsx::addStyle(wb,     "Inwoners_1950_1977", style = s, rows=2:5377, cols=4, gridExpand = TRUE)
  openxlsx::addStyle(wb,     "Inwoners_1978_1993", style = s, rows=2:1048576, cols=9, gridExpand = TRUE)
  openxlsx::addStyle(wb,     "Inwoners_1994_2009", style = s, rows=2:1048576, cols=9, gridExpand = TRUE)
  openxlsx::addStyle(wb,     "Inwoners_2010_2024", style = s, rows=2:1048576, cols=9, gridExpand = TRUE)
  openxlsx::setColWidths(wb, "Gemeente_historisch", cols=1:9, widths="auto")
  openxlsx::setColWidths(wb, "Inwoners_1950_1977", cols=1:4, widths="auto")
  openxlsx::setColWidths(wb, "Inwoners_1978_1993", cols=1:9, widths="auto")
  openxlsx::setColWidths(wb, "Inwoners_1994_2009", cols=1:9, widths="auto")
  openxlsx::setColWidths(wb, "Inwoners_2010_2024", cols=1:9, widths="auto")
  openxlsx::saveWorkbook(wb, file=file.path("//HERA/KISS/Qlik/ods","Inwoners_gemeente.xlsx"), overwrite=TRUE)
  #options("openxlsx.numFmt" = NULL)
}  

Inwoners_gemeente_historisch <- Inwoners1950 %>%
  group_by(Jaar, Provincie, Eenheid, Politieregio, gemeente_nr, Gemeente_naam, gemeente_nr_recent, Gemeente_naam_recent) %>%
  summarize(Inwoners=sum(Inwoners), .groups='drop') # 25839

  count(Inwoners1950, Gemeente_naam, Gemeente_naam_recent, wt=Inwoners) #991 --> #344
  print(pivot_wider(count(Inwoners1950_recent_lft, Jaar, Provincie, wt=Inwoners), names_from=Provincie, values_from=n), n=10000)
  print(pivot_wider(count(Inwoners1950_recent_lft, Jaar, Eenheid, wt=Inwoners), names_from=Eenheid, values_from=n), n=10000)

  #write.xlsx(Inwoners_gemeente_historisch, file=file.path("//HERA/KISS/Qlik/ods","Inwoners_gemeente_historisch.xlsx"))
  #write.csv2(Inwoners1950_oms[Inwoners1950_oms$Jaar<2000,], file=file.path("//HERA/KISS/Qlik/ods","Inwoners_1950_1999.csv"), row.names=FALSE)

Inwoners_regio_leeftijd <- Inwoners1950 %>%
    group_by(Jaar, Provincie, Eenheid, Politieregio, Geslacht, Sex, Leeftijd=lft_inw) %>%
    summarize(Inwoners=sum(Inwoners), .groups='drop') # 51185
  
  count(Inwoners_regio_leeftijd, Jaar, wt=Inwoners) 
  print(pivot_wider(count(Inwoners_regio_leeftijd, Jaar, Provincie, wt=Inwoners), names_from=Provincie, values_from=n), n=10000)
  print(pivot_wider(count(Inwoners_regio_leeftijd, Jaar, Eenheid, wt=Inwoners), names_from=Eenheid, values_from=n), n=10000)
  print(pivot_wider(count(Inwoners_regio_leeftijd, Politieregio, Provincie, wt=Inwoners), names_from=Provincie, values_from=n), n=10000)
  
  
  
#Gehele bestand (recent_lft)
  names(Inwoners1950_recent_lft)
  nrow(Inwoners1950_recent_lft)
write.csv2(Inwoners1950_recent_lft, file=file.path("//HERA/KISS/Qlik/ods","Inwoners_gemeente_recent_lft.csv"), row.names=FALSE)

#Let op het blad Toelichting
Toelichting <- read_ods(file.path("//HERA/KISS/Qlik/ods","Inwoners_vanaf_1950_gemeente_leeftijd_geslacht.ods"),sheet="Toelichting", row_names = FALSE, col_names = FALSE)
write.csv2(Toelichting,      file=file.path("//HERA/KISS/Qlik/ods","Inwoners_vanaf_1950_Toelichting.csv"), row.names=FALSE)

write_ods(list("Toelichting"              = Toelichting,
               "Gemeenten_historisch"     = Inwoners_gemeente_historisch,
               "Gemeenten_recent_lft_sex" = Inwoners1950_recent_lft,
               "Inwoners_regio_leeftijd"  = Inwoners_regio_leeftijd),
          path=file.path("//HERA/KISS/Qlik/ods","Inwoners_vanaf_1950_gemeente_leeftijd_geslacht.ods"))
#kopieer nog de eerste rij met engelse kolomnamen uit de voerige versie

rm(Inwoners_cbs, Inwoners1950, Trans_gemeente, Trans_leeftijd, Gemeente_recent, losseleeftijden)
rm(codeboek, ref_eenheid, ref_geslacht, ref_politie_regio, ref_provincie)
rm(Inwoners_gemeente_historisch, Inwoners1950_recent_lft, Inwoners_regio_leeftijd)
gc()
#############################################################################################################

##### REGISTRATIEGRAAD ######################################################################################
  WisdomTabellen %>% filter(grepl("code", tabelnaam))
  dbListFields(conUR, "codeboek")
  
  WisdomTabellen %>% filter(grepl("IVO2", tabelnaam))
  dbListFields(conUR, "IVO2_DAG_SLA")
  dbListFields(conUR, "IVO2_LEEFTIJD_SLA")
  dbListFields(conUR, "IVO2_LFT_VVM")
  dbListFields(conUR, "IVO2_MAAND_SLA")
  dbListFields(conUR, "IVO2_PROV_SLA_DZ")
  dbListFields(conUR, "IVO2_SEX_SLA")
  dbListFields(conUR, "IVO2_UUR_SLA")
  dbListFields(conUR, "IVO2_VVM_SLA")

# selecteer codeboeken
  #View op tabellen, variabelen en codeboeken: SHOW_TABLE_CONTENT
#  qry <- "SELECT table_name, create_date, modify_date, [Column Name], [Data type] codeboek_var_nr FROM SHOW_TABLE_CONTENT"
#  Table_Content <- DBI::dbGetQuery(conUR, qry)
#  names(Table_Content)
  # deze geeft per var_naam aan wat de var_nr is. Het codeboek kan bij een andere var staan (bijv ote_id [1087] gwortdt gebruikt in 1140, 1119 em 1120 (otesl, _A, _B)
  # naast codeboek zijn er voor veel variabelen al REF-tabellen
  
  qry <- "SELECT var_nr, var_naam, cob_code AS code, cob_label as oms FROM codeboek WHERE var_naam IN ('JAAR', 'LETSELERNST_IVO2', 'SLA_IVO', 'SLA_VOR', 'WEEKDAG', 'LEEFTIJD_IVO2', 'KLEEFT', 'MAAND', 'PROV_IVO', 'SEXE', 'UUR_IVO2', 'VVM_IVO', 'VVM_NM')"
  codeboek <- DBI::dbGetQuery(conUR, qry)
  (nummers <- unique(codeboek$var_nr))
  char <- ""
  for (i in 1:length(nummers)) {char=paste0(char, nummers[i], sep=",")}
  (char=substring(char,1,nchar(char)-1))
  (qry <- paste0("SELECT var_nr, var_naam, cob_code AS code, cob_label as oms, cob_labelgb as omsgb, cob_omschrijving FROM codeboek WHERE var_nr IN (", char, ")"))
  codeboek <- DBI::dbGetQuery(conUR, qry)
  count(codeboek, var_nr, var_naam)
  
  (ref_dag <- codeboek %>%
      filter(var_naam=="weekdag" | var_nr==200) %>%
      rename(WEEKDAG=code, Weekdag=oms, Weekday=omsgb) %>%
      mutate(WEEKDAG=as.integer(WEEKDAG)))
  (ref_leeftijd <- codeboek %>%
      filter(var_naam=="leeftijd_ivo2" | var_nr==929) %>%
      mutate(LEEFTIJD_IVO2=as.integer(code)) %>%
      arrange(LEEFTIJD_IVO2) %>%
      rename(Leeftijd=oms, Agegroup=omsgb) %>%
      select(-code))
  (ref_maand <- codeboek %>%
      filter(var_naam=="maand" | var_nr==419) %>%
      mutate(MAAND=as.integer(code)) %>%
      arrange(MAAND) %>%
      rename(Maand=oms, Month=omsgb) %>%
      select(-code))
  (ref_prov <- codeboek %>%
      filter(var_naam=="prov_ivo" | var_nr==556) %>%
      mutate(PROV_IVO=as.integer(code)) %>%
      arrange(PROV_IVO) %>%
      rename(Provincie=oms) %>%
      select(-code) %>%
      mutate(Provincie=case_when(
        Provincie=="FryslÃ¢n" ~ "Fryslân",
        Provincie=="Overyssel" ~ "Overijssel",
        Provincie=="Overyssel + Flevoland" ~ "Overijssel + Flevoland",
        TRUE ~ Provincie)))
  (ref_sex <- codeboek %>%
      filter(var_naam=="sexe" | var_nr==264) %>%
      mutate(SEXE=as.integer(code)) %>%
      arrange(SEXE) %>%
      rename(Geslacht=oms, Sex=omsgb) %>%
      select(-code))
  (ref_uur <- codeboek %>%
      filter(var_naam=="uur_ivo2" | var_nr==930) %>%
      mutate(UUR_IVO2=as.integer(code)) %>%
      arrange(UUR_IVO2) %>%
      rename(Tijdstip=oms, Time=omsgb) %>%
      select(-code))
  (ref_vvm <- codeboek %>%
      filter(var_naam=="vvm_ivo" | var_nr==551) %>%
      mutate(VVM_IVO=as.integer(code)) %>%
      arrange(VVM_IVO) %>%
      rename(Vervoerswijze=oms, TransportMode=omsgb) %>%
      select(-code))
  
    
# selecteer data  
Registratiegraad_dag <- dbReadTable(conUR, "IVO2_DAG_SLA") %>%
  filter(LETSELERNST_IVO2==1 & JAAR>1995) %>%
  select(-ROWID, -LETSELERNST_IVO2) %>%
  rename(Geregistreerd=SLA_VOR, Werkelijk=SLA_IVO, Jaar=JAAR) %>%
  mutate(Registratiegraad=round(100*Geregistreerd/Werkelijk,digits=1)) %>%
  left_join(ref_dag %>% select(-var_nr, -var_naam),by = join_by(WEEKDAG)) %>%
  filter(Jaar<2022) %>%
  select(-WEEKDAG) %>%
  select(Jaar, Weekdag, Weekday, Werkelijk, Geregistreerd, Registratiegraad)

  Registratiegraad_dag
  pivot_wider(Registratiegraad_dag %>% select(-Werkelijk, -Registratiegraad) , names_from=Jaar, values_from=Geregistreerd)
  pivot_wider(Registratiegraad_dag %>% select(-Geregistreerd, -Registratiegraad) , names_from=Jaar, values_from=Werkelijk)
  pivot_wider(Registratiegraad_dag %>% select(-Geregistreerd, -Werkelijk) , names_from=Jaar, values_from=Registratiegraad)

Registratiegraad_leeftijd <- dbReadTable(conUR, "IVO2_LEEFTIJD_SLA") %>%
  filter(LETSELERNST_IVO2==1 & JAAR>1995) %>%
  select(-ROWID, -LETSELERNST_IVO2) %>%
  rename(Geregistreerd=SLA_VOR, Werkelijk=SLA_IVO, Jaar=JAAR) %>%
  mutate(Registratiegraad=round(100*Geregistreerd/Werkelijk,digits=1)) %>%
  left_join(ref_leeftijd %>% select(-var_nr, -var_naam),by = join_by(LEEFTIJD_IVO2)) %>%
  arrange(Jaar, LEEFTIJD_IVO2) %>%
  select(-LEEFTIJD_IVO2) %>%
  select(Jaar, Leeftijd, Agegroup, Werkelijk, Geregistreerd, Registratiegraad)

  Registratiegraad_leeftijd
  pivot_wider(Registratiegraad_leeftijd %>% select(-Werkelijk, -Registratiegraad) , names_from=Jaar, values_from=Geregistreerd)
  pivot_wider(Registratiegraad_leeftijd %>% select(-Geregistreerd, -Registratiegraad) , names_from=Jaar, values_from=Werkelijk)
  pivot_wider(Registratiegraad_leeftijd %>% select(-Geregistreerd, -Werkelijk) , names_from=Jaar, values_from=Registratiegraad)

  
Registratiegraad_maand <- dbReadTable(conUR, "IVO2_MAAND_SLA") %>%
    filter(LETSELERNST_IVO2==1 & JAAR>1995) %>%
    #select(-ROWID)
    select(-LETSELERNST_IVO2) %>%
    rename(Geregistreerd=SLA_VOR, Werkelijk=SLA_IVO, Jaar=JAAR) %>%
    mutate(Registratiegraad=round(100*Geregistreerd/Werkelijk,digits=1)) %>%
    left_join(ref_maand %>% select(-var_nr, -var_naam),by = join_by(MAAND)) %>%
    arrange(Jaar, MAAND) %>%
    select(-MAAND) %>%
    select(Jaar, Maand, Month, Werkelijk, Geregistreerd, Registratiegraad)

  #Registratiegraad_maand
  pivot_wider(Registratiegraad_maand %>% select(-Werkelijk, -Registratiegraad) , names_from=Jaar, values_from=Geregistreerd)
  pivot_wider(Registratiegraad_maand %>% select(-Geregistreerd, -Registratiegraad) , names_from=Jaar, values_from=Werkelijk)
  pivot_wider(Registratiegraad_maand %>% select(-Geregistreerd, -Werkelijk) , names_from=Jaar, values_from=Registratiegraad)
  
  
Registratiegraad_prov <- dbReadTable(conUR, "IVO2_PROV_SLA_DZ") %>%
    filter(LETSELERNST_IVO2==1 & JAAR>1995) %>%
    #select(-ROWID)
    select(-LETSELERNST_IVO2) %>%
    rename(Geregistreerd=SLA_VOR, Werkelijk=SLA_IVO, Jaar=JAAR) %>%
    mutate(Registratiegraad=round(100*Geregistreerd/Werkelijk,digits=1)) %>%
    left_join(ref_prov %>% select(-var_nr, -var_naam),by = join_by(PROV_IVO)) %>%
    arrange(Jaar, PROV_IVO) %>%
    select(-PROV_IVO) %>%
    select(Jaar, Provincie, Werkelijk, Geregistreerd, Registratiegraad)
  
  pivot_wider(Registratiegraad_prov %>% select(-Werkelijk, -Registratiegraad) , names_from=Jaar, values_from=Geregistreerd)
  pivot_wider(Registratiegraad_prov %>% select(-Geregistreerd, -Registratiegraad) , names_from=Jaar, values_from=Werkelijk)
  pivot_wider(Registratiegraad_prov %>% select(-Geregistreerd, -Werkelijk) , names_from=Jaar, values_from=Registratiegraad)
  
Registratiegraad_sex <- dbReadTable(conUR, "IVO2_SEX_SLA") %>%
    filter(LETSELERNST_IVO2==1 & JAAR>1995) %>%
    #select(-ROWID)
    select(-LETSELERNST_IVO2) %>%
    rename(Geregistreerd=SLA_VOR, Werkelijk=SLA_IVO, Jaar=JAAR) %>%
    mutate(Registratiegraad=round(100*Geregistreerd/Werkelijk,digits=1)) %>%
    left_join(ref_sex %>% select(-var_nr, -var_naam),by = join_by(SEXE)) %>%
    arrange(Jaar, SEXE) %>%
    select(-SEXE) %>%
    select(Jaar, Geslacht, Sex, Werkelijk, Geregistreerd, Registratiegraad)
  
  pivot_wider(Registratiegraad_sex %>% select(-Werkelijk, -Registratiegraad) , names_from=Jaar, values_from=Geregistreerd)
  pivot_wider(Registratiegraad_sex %>% select(-Geregistreerd, -Registratiegraad) , names_from=Jaar, values_from=Werkelijk)
  pivot_wider(Registratiegraad_sex %>% select(-Geregistreerd, -Werkelijk) , names_from=Jaar, values_from=Registratiegraad)
  
Registratiegraad_uur <- dbReadTable(conUR, "IVO2_UUR_SLA") %>%
    filter(LETSELERNST_IVO2==1 & JAAR>1995) %>%
    select(-ROWID, -LETSELERNST_IVO2) %>%
    rename(Geregistreerd=SLA_VOR, Werkelijk=SLA_IVO, Jaar=JAAR) %>%
    mutate(Registratiegraad=round(100*Geregistreerd/Werkelijk,digits=1)) %>%
    left_join(ref_uur %>% select(-var_nr, -var_naam),by = join_by(UUR_IVO2)) %>%
    arrange(Jaar, UUR_IVO2) %>%
    select(-UUR_IVO2) %>%
    select(Jaar, Tijdstip, Time, Werkelijk, Geregistreerd, Registratiegraad)
  
  pivot_wider(Registratiegraad_uur %>% select(-Werkelijk, -Registratiegraad) , names_from=Jaar, values_from=Geregistreerd)
  pivot_wider(Registratiegraad_uur %>% select(-Geregistreerd, -Registratiegraad) , names_from=Jaar, values_from=Werkelijk)
  pivot_wider(Registratiegraad_uur %>% select(-Geregistreerd, -Werkelijk) , names_from=Jaar, values_from=Registratiegraad)
  
Registratiegraad_vvm <- dbReadTable(conUR, "IVO2_VVM_SLA") %>%
    filter(LETSELERNST_IVO2==1 & JAAR>1995) %>%
    select(-ROWID, -LETSELERNST_IVO2) %>%
    rename(Geregistreerd=SLA_VOR, Werkelijk=SLA_IVO, Jaar=JAAR) %>%
    mutate(Registratiegraad=round(100*Geregistreerd/Werkelijk,digits=1)) %>%
    left_join(ref_vvm %>% select(-var_nr, -var_naam),by = join_by(VVM_IVO)) %>%
    arrange(Jaar, VVM_IVO) %>%
    select(-VVM_IVO) %>%
    select(Jaar, Vervoerswijze, TransportMode, Werkelijk, Geregistreerd, Registratiegraad)
  
  pivot_wider(Registratiegraad_vvm %>% select(-Werkelijk, -Registratiegraad) , names_from=Jaar, values_from=Geregistreerd)
  pivot_wider(Registratiegraad_vvm %>% select(-Geregistreerd, -Registratiegraad) , names_from=Jaar, values_from=Werkelijk)
  pivot_wider(Registratiegraad_vvm %>% select(-Geregistreerd, -Werkelijk) , names_from=Jaar, values_from=Registratiegraad)

# controleer totalen en jaren WERKELIJK
  dag <- count(Registratiegraad_dag, Jaar, wt=Werkelijk)
  leeftijd <- count(Registratiegraad_leeftijd, Jaar, wt=Werkelijk)
  maand <- count(Registratiegraad_maand, Jaar, wt=Werkelijk)
  prov <- count(Registratiegraad_prov, Jaar, wt=Werkelijk)
  sex <- count(Registratiegraad_sex, Jaar, wt=Werkelijk)
  uur <- count(Registratiegraad_uur, Jaar, wt=Werkelijk)
  vvm <- count(Registratiegraad_vvm, Jaar, wt=Werkelijk)
  (alles <- left_join(vvm, dag, by=join_by(Jaar)) %>% rename(vvm=n.x, dag=n.y) %>%
    left_join(leeftijd, by=join_by(Jaar)) %>% rename(leeftijd=n) %>%
    left_join(maand, by=join_by(Jaar)) %>% rename(maand=n) %>%
    left_join(prov, by=join_by(Jaar)) %>% rename(prov=n) %>%
    left_join(sex, by=join_by(Jaar)) %>% rename(sex=n) %>%
    left_join(uur, by=join_by(Jaar)) %>% rename(uur=n))

# controleer totalen en jaren GEREGISTREERD
  dag <- count(Registratiegraad_dag, Jaar, wt=Geregistreerd)
  leeftijd <- count(Registratiegraad_leeftijd, Jaar, wt=Geregistreerd)
  maand <- count(Registratiegraad_maand, Jaar, wt=Geregistreerd)
  prov <- count(Registratiegraad_prov, Jaar, wt=Geregistreerd)
  sex <- count(Registratiegraad_sex, Jaar, wt=Geregistreerd)
  uur <- count(Registratiegraad_uur, Jaar, wt=Geregistreerd)
  vvm <- count(Registratiegraad_vvm, Jaar, wt=Geregistreerd)
  (alles <- left_join(vvm, dag, by=join_by(Jaar)) %>% rename(vvm=n.x, dag=n.y) %>%
      left_join(leeftijd, by=join_by(Jaar)) %>% rename(leeftijd=n) %>%
      left_join(maand, by=join_by(Jaar)) %>% rename(maand=n) %>%
      left_join(prov, by=join_by(Jaar)) %>% rename(prov=n) %>%
      left_join(sex, by=join_by(Jaar)) %>% rename(sex=n) %>%
      left_join(uur, by=join_by(Jaar)) %>% rename(uur=n))
  
Registratiegraad_dag <- Registratiegraad_dag %>% filter(Jaar<2022)
Registratiegraad_uur <- Registratiegraad_uur %>% filter(Jaar<2022 & Jaar>2000)

Toelichting <- read_ods(file.path("//HERA/KISS/Qlik/ods","Registratiegraad.ods"),sheet="Toelichting", row_names = FALSE, col_names = FALSE)
Explanation <- read_ods(file.path("//HERA/KISS/Qlik/ods","Registratiegraad.ods"),sheet="Explanation", row_names = FALSE, col_names = FALSE)
write.csv2(Toelichting,      file=file.path("//HERA/KISS/Qlik/ods","Registratiegraad_Toelichting.csv"), row.names=FALSE)
write.csv2(Explanation,      file=file.path("//HERA/KISS/Qlik/ods","Registratiegraad_Explanation.csv"), row.names=FALSE)

# schrijf data weg als csv/odt
write.csv2(Registratiegraad_dag,      file=file.path("//HERA/KISS/Qlik/ods","Registratiegraad_dag.csv"), row.names=FALSE)
write.csv2(Registratiegraad_leeftijd, file=file.path("//HERA/KISS/Qlik/ods","Registratiegraad_leeftijd.csv"), row.names=FALSE)
write.csv2(Registratiegraad_maand,    file=file.path("//HERA/KISS/Qlik/ods","Registratiegraad_maand.csv"), row.names=FALSE)
write.csv2(Registratiegraad_prov,     file=file.path("//HERA/KISS/Qlik/ods","Registratiegraad_prov.csv"), row.names=FALSE)
write.csv2(Registratiegraad_sex,      file=file.path("//HERA/KISS/Qlik/ods","Registratiegraad_sex.csv"), row.names=FALSE)
write.csv2(Registratiegraad_uur,      file=file.path("//HERA/KISS/Qlik/ods","Registratiegraad_uur.csv"), row.names=FALSE)
write.csv2(Registratiegraad_vvm,      file=file.path("//HERA/KISS/Qlik/ods","Registratiegraad_vvm.csv"), row.names=FALSE)

write_ods(list("Toelichting"  = Toelichting,
               "Explanation"  = Explanation,
               "Maand"        = Registratiegraad_maand,
               "Dag"          = Registratiegraad_dag, 
               "Tijdstip"     = Registratiegraad_uur,
               "Provincie"    = Registratiegraad_prov,
               "Vervoerswijze"= Registratiegraad_vvm,
               "Leeftijd"     = Registratiegraad_leeftijd,
               "Geslacht"     = Registratiegraad_sex),
          path=file.path("//HERA/KISS/Qlik/ods","Registratiegraad.ods"))


#############################################################################################################

#### ONGEVALLEN naar WEGBEHEERDER en GEMEENTE/REGIO vanaf 1976/1987 ####
#
# Zie regel 140-230 voor transformatie Gemeente, Prov, Polreg, Eenheid en inlezen codeboek wegbeheerder
#

vars <- paste0("Jaar, ernong5, key_gem, prov, wegbeh, loctypon, bebouw, PVrapp")
qry <- paste0("SELECT ", vars, " FROM BRON_ONGEVAL WHERE JAAR>1986") 
Ong_BRON <- DBI::dbGetQuery(conn=conUR, qry) # 6067426
Ong_BRON <- Ong_BRON  %>%
  mutate(pv=case_when(
    PVrapp %in% c(1,2) ~ "PV/Regset/KMM+",
    PVrapp %in% c(6) ~ "PV/Regset/KMM+",
    PVrapp %in% c(7) ~ "PV/Regset/KMM+",
    ernong5!=5 ~ "PV/Regset/KMM+", # 8 cases
    PVrapp %in% c(9) ~ "KMM",
    PVrapp %in% c(10,11) ~ "IM")) %>%
  mutate(Ernst_ongeval=case_when(
    ernong5==1 ~ 'Dodelijk',
    ernong5==5 ~ 'UMS',
    TRUE ~ 'Letsel')) %>%
  mutate(bebouw=as.character(bebouw),
         loctypon=as.character(loctypon),
         wegbeh=as.character(wegbeh))  %>%
  left_join(Trans_gemeente, by = join_by(key_gem==gemeente_nr)) %>%
  left_join(codeboek[codeboek$var_nr==203,c(3,4)], by=join_by(wegbeh==code)) %>% rename(Wegbeheerder=oms) %>%
  select(-ernong5, -loctypon, -bebouw, -PVrapp) %>%
  #select(-key_gem, -Gemeente_naam, -wegbeh, ) %>%
  # behoud de oorspronkelijke gemeente (naar huidige provincie)
  group_by(Jaar, Ernst_ongeval, pv, gemeente_nr_recent, Gemeente_naam_recent, Gemeente_naam, Provincie, Politieregio, Eenheid, Wegbeheerder) %>%
  summarize(Aantal=n(), .groups='drop') # 121511

# Ongevallen 1976-1986
dbListFields(conUR, "VOR_ONG_COG")
vars_VOR <- paste0("Jaar, gemeente_nr, wegbeh, loctypon, bebouw, Ndood")
qry_vor <- paste0("SELECT ", vars_VOR, " FROM VOR_ONG_COG  WHERE Jaar<1987") 
# # ongeveer identiek in de periode 1987-2003, 
#     muv 30 ongevallen in 1992 en 14 in 1993 (naijlers met resp 32 en 17 extra doden (en 7+10 gewonden)).
#     muv de gemeente van ongeval; die is obv recente polygonen opnieuw ingedeeld naar de huidige gemeente dd 2004
#        bijvoorbeeld Den Haag kreeg met Leidschenveen/PrinsClausplein ook een stuk van de A12 en had daardoor ineens veel meer rijkswegongevallen
#        voor de langjarige vergelijking is dat wel zo handig
#        neem dus alleen de VOR-reeks 1976-1986 en pak BRON vanaf 1987
# 
Ong_VOR <- DBI::dbGetQuery(conUR, qry_vor) # 529648
# Naijers
  xong92 <- readRDS("//hera/kiss/vor/NAIJL/xong92.rds")
  xong93 <- readRDS("//hera/kiss/vor/NAIJL/xong93.rds")
  Naijlers <- rbind(xong92, xong93) %>%
    select(Jaar=JAAR, gemeente_nr=KEY_GEM, wegbeh=WEGBEH, loctypon=LOCTYPON, bebouw=BEBOUW, Ndood=NDOOD) %>%
    mutate(Jaar=if_else(Jaar==92, 1992, 1993)) # 44
  
Ong_VORn <- rbind(Ong_VOR %>% filter(Jaar<1987), Naijlers) %>%
  mutate(pv="PV/Regset/KMM+",
    Ernst_ongeval=case_when(
    Ndood==0 ~ "Letsel",
    TRUE ~ "Dodelijk")) %>% # er is 1 ongeval in 1983 met Ndood=0 en maxlet=0 -> letselongeval
  mutate(bebouw=as.character(bebouw),
         loctypon=as.character(loctypon),
         wegbeh=as.character(wegbeh))  %>%
  left_join(codeboek[codeboek$var_nr==203,c(3,4)], by=join_by(wegbeh==code)) %>% rename(Wegbeheerder=oms) %>% # zelfde codeboek VOR==BRON
  mutate(Wegbeheerder=case_when(
    Wegbeheerder=="Spoorwegen" ~ "Overige instanties",
    Wegbeheerder=="Staatsbosbeheer" ~ "Overige instanties",
    TRUE ~ Wegbeheerder)) %>%
  select(Jaar, Ernst_ongeval, pv, Wegbeheerder, gemeente_nr) %>%
  left_join(Trans_gemeente, by = join_by(gemeente_nr)) %>%
  group_by(Jaar, Ernst_ongeval, pv, Wegbeheerder, Gemeente_naam, gemeente_nr_recent, Gemeente_naam_recent, Provincie, Politieregio, Eenheid) %>%
  summarize(Aantal=n(), .groups='drop') # 252426 / 25469

count(Ong_VORn, Wegbeheerder, wt=Aantal)
# vergelijk aantallen per gemeente in VOR en BRON 1987-2003
#count(Ong_VORn[Ong_VORn$Jaar %in% c(1987:2003),], Jaar, Ernst_ongeval, wt=Aantal)
# as.data.frame(pivot_wider(count(Ong_VOR[Ong_VOR$Jaar %in% c(1987:2003),], Jaar, Ernst_ongeval, wt=Aantal), names_from=Jaar, values_from=n))[,2:17] -
#   as.data.frame(pivot_wider(count(Ong_BRON[Ong_BRON$Jaar %in% c(1987:2003) & Ong_BRON$Ernst_ongeval!="UMS",], Jaar, Ernst_ongeval, wt=Aantal), names_from=Jaar, values_from=n)[,2:17])
# 
# v <-as.data.frame(count(Ong_VOR[Ong_VOR$Jaar %in% c(1987:2003),], Jaar, Provincie, Gemeente_naam_recent, Gemeente_naam, wt=Aantal))
# b <-  as.data.frame(count(Ong_BRON[Ong_BRON$Jaar %in% c(1987:2003) & Ong_BRON$Ernst_ongeval!="UMS",], Jaar, Provincie, Gemeente_naam_recent, Gemeente_naam, wt=Aantal))
# write.csv2(v, file=file.path("//HERA/KISS/Qlik/ods","v.csv"), row.names=FALSE) 
# write.csv2(b, file=file.path("//HERA/KISS/Qlik/ods","b.csv"), row.names=FALSE) 
# 

pivot_wider(count(Ong_VORn, Jaar, Wegbeheerder, wt=Aantal), names_from=Jaar, values_from=n)
pivot_wider(count(Ong_VORn, Jaar, Provincie, wt=Aantal), names_from=Jaar, values_from=n)
pivot_wider(count(Ong_VORn, Jaar, Eenheid, wt=Aantal), names_from=Jaar, values_from=n)
pivot_wider(count(Ong_VORn, Jaar, Politieregio, wt=Aantal), names_from=Jaar, values_from=n)


count(Ongevallen_BRON, PVrapp)

Ongevallen_recent <- bind_rows(Ong_VORn, Ong_BRON)

pivot_wider(count(Ongevallen_recent[Ongevallen_recent$Jaar>2008,], Jaar, pv, wt=Aantal), names_from=Jaar, values_from=n)

# getOption("encoding") #"native.enc" werkt ook, maar excel importeert het fout. Gebruik Gegevens ophalen
# getOption("native.enc") #NULL 
#iconvlist() #374
write.csv2(Ongevallen_recent, file=file.path("//HERA/KISS/Qlik/ods","Ongevallen_recent.csv"), row.names=FALSE) #, fileEncoding = "UTF8") # is ook gewoon goed

####