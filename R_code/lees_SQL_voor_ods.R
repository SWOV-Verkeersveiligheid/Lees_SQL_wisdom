#############################################################################################################
# lees_SQL database.R
#
#
# versie wie    wat
# 0.1    Niels  dbReadTable
# 0.2    Niels  dbGetQuery, export van Verkeersdoden vanaf 1950
# 0.3    Niels  export Inwoners 1950 naar ods
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

# lees- en schrijfrechten op de wisdom DB in Uranus server
update_DB <- FALSE
source("//hera/kiss/restricted/AssignWisdom_Mod_uranus.R")

WisdomTabellen <- as.data.frame(odbc::dbListTables(conUW, catalog = 'Wisdom', schema = 'Admin1')) #497
names(WisdomTabellen) <- "tabelnaam"
  Tabellen %>% filter(grepl("BRON", tabelnaam))

  dbListFields(conUW, "BRON_SLACHTOFFER")
  dbListFields(conUW, "BRON_OBJECT")
  dbListFields(conUW, "BRON_ONGEVAL")


#### Voorbeeld dbReadTable of dbGetQuery ####################################################################
  dbListFields(conUW, "BRON_ONGEVAL")
Ongevallen <- dbReadTable(conUW, "BRON_ONGEVAL") %>% # eerste helemaal binnenhalen
  filter(WEGBEH==1) %>%                             # en dan pas filteren
  filter(JAAR>2022)

  names(Ongevallen)
  table(Ongevallen[Ongevallen$OTE_A==94,]$VERVOERSW_SWOV_A, Ongevallen[Ongevallen$OTE_A==94,]$ERNONG5)

# Een sql query gaat sneller, en je kunt bepalen of de variabelen in hoofd- of kleine letters staan
vars <- paste0("jaar, ernong5, VORNUM, datum, niveaukop, wvk_idwegvak=wvk_id, x, y, wegnummer, hectometer, wegbeh, ",
               "wegsoort, WVK_ID=wvk_id1, loctypon, bst_code, bebouw, ote_A, ote_B, vervoersw_swov_A, vervoersw_swov_B")
qry <- paste0("SELECT ", vars, " FROM BRON_ONGEVAL WHERE JAAR>2007")
Ongevallen <- DBI::dbGetQuery(conn=conUW, qry)

  table(Ongevallen[Ongevallen$ote_A==94,]$vervoersw_swov_A, Ongevallen[Ongevallen$ote_A==94,]$ernong5)

#### Als QAP vervanging cvs/ods maken voor relevante tabellen, incl codeboek ################################

##### VERKEERSDODEN VANAF 1950 ##############################################################################

tabellen %>% filter(grepl("DODEN", tabelnaam))
dbListFields(conUW, "DODEN_LFT_WVD_TIJD")

qry <- "SELECT var_nr, var_naam, cob_code AS code, cob_label as oms FROM codeboek WHERE var_naam IN ('leeftijd_4_75', 'vervoer2')"
codeboek <- DBI::dbGetQuery(conUW, qry)
ref_leeftijd_4_75 <- codeboek %>%
  filter(var_naam=="leeftijd_4_75") %>%
  rename(LEEFTIJD_4_75=code, Leeftijdsklasse=oms) %>%
  mutate(LEEFTIJD_4_75=as.integer(LEEFTIJD_4_75))
ref_vervoer2 <- codeboek %>%
  filter(var_naam=="vervoer2") %>%
  rename(VERVOER2=code, Vervoerswijze=oms) %>%
  mutate(VERVOER2=as.integer(VERVOER2))

  
Doden1950 <- dbReadTable(conUW, "DODEN_LFT_WVD_TIJD") %>%
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
  select(Jaar, Leeftijdsklasse, Vervoerswijze, Verkeersdoden)


count(Doden1950_oms, Leeftijdsklasse, wt=Verkeersdoden)
count(Doden1950_oms, Vervoerswijze, wt=Verkeersdoden)

write.csv2(Doden1950_oms, file=file.path("//HERA/KISS/Qlik/ods","Doden_1950.csv"), row.names=FALSE)
write_ods(Doden1950_oms, path=file.path("//HERA/KISS/Qlik/ods","Doden_1950.ods"), sheet = "Doden_1950")
          

##### BEVOLKING VANAF 1950 ##################################################################################

  Tabellen %>% filter(grepl("INWON", tabelnaam))
  dbListFields(conUW, "INWONERS_CBS")
  #dbListFields(conUW, "INWONERS_CBS_VIEW")
  #dbListFields(conUW, "CBS_INWONERS_LEEFTIJD_GESLACHT")
  Tabellen %>% filter(grepl("GEME", tabelnaam))
  dbListFields(conUW, "GEMEENTEN_LIJST")

qry <- "SELECT var_nr, var_naam, cob_code AS code, cob_label as oms FROM codeboek WHERE var_naam IN ('geslacht', 'gemeente_nr')"
codeboek <- DBI::dbGetQuery(conUW, qry)
ref_geslacht <- codeboek %>%
  filter(var_naam=="geslacht") %>%
  rename(geslacht=code, Geslacht=oms) %>%
  mutate(geslacht=as.integer(geslacht))

# ref_gemeente <- codeboek %>%
#   filter(var_naam=="gemeente_nr") %>%
#   rename(gemeente_nr=code, Gemeente_naam=oms) %>%
#   mutate(gemeente_nr=as.integer(gemeente_nr))

Trans_gemeente <- dbReadTable(conUW, "GEMEENTEN_LIJST") %>% 
  select(gemeente_nr=GEMEENTE_NR, Gemeente_naam=GEMEENTE_NAAM, Gemeente_recent=GEMEENTEN_RECENT, Provincie=PROVINCIE_ONBEKEND, Politieregio=POLITIE_REGIO, Eenheid=ARRONDISSEMENT)

  names(Trans_gemeente)
  
Gemeente_recent <- Trans_gemeente %>%
  select(Gemeente_recent) %>% unique() %>% # 358
  left_join(Trans_gemeente %>% select(-Gemeente_recent), by = join_by(Gemeente_recent==gemeente_nr))

  names(Gemeente_recent)
  
Trans_gemeente <- Trans_gemeente %>% select(-Provincie, -Politieregio, -Eenheid) %>%
  left_join(Gemeente_recent %>% rename(Gemeente_naam_recent=Gemeente_naam), by = join_by(Gemeente_recent))

  names(Trans_gemeente)

Inwoners_cbs <- dbReadTable(conUW, "INWONERS_CBS") %>%
  select(-ROWID) # 13.083.021

  names(Inwoners_cbs)
  
Inwoners1950 <- Inwoners_cbs  %>%
  group_by(jaar, geslacht, lft_inw, gemeente_nr) %>%
  summarize(Inwoners=sum(aantal, na.rm=TRUE), .groups='drop') %>%
  left_join(ref_geslacht, by = join_by(geslacht)) %>%
  select(-var_nr, -var_naam, -geslacht) %>%
  left_join(Trans_gemeente, by = join_by(gemeente_nr)) %>%
  arrange(    jaar, Provincie, Eenheid, Politieregio, gemeente_nr, Gemeente_naam, Gemeente_recent, Gemeente_naam_recent, Geslacht, lft_inw) %>%
  select(Jaar=jaar, Provincie, Eenheid, Politieregio, Gemeente_nr=gemeente_nr, Gemeente_naam, Gemeente_recent, Gemeente_naam_recent, Geslacht, lft_inw, Inwoners) %>% 
  mutate(Provincie            = if_else(Jaar<1978, 99, Provincie),
         Eenheid              = if_else(Jaar<1978, 11, Eenheid),
         Politieregio         = if_else(Jaar<1978, 99, Politieregio),
         Gemeente_nr          = if_else(Jaar<1978, 9999, Gemeente_nr),
         Gemeente_naam        = if_else(Jaar<1978, "Onbekend", Gemeente_naam),
         Gemeente_recent      = if_else(Jaar<1978, 9999, Gemeente_recent), 
         Gemeente_naam_recent = if_else(Jaar<1978, "Onbekend", Gemeente_naam_recent))

  #View(Inwoners1950[Inwoners1950$Jaar<1978 & Inwoners1950$lft_inw<16 & Inwoners1950$Geslacht=="Man",])
  #View(Inwoners1950[Inwoners1950$Jaar>1977 & Inwoners1950$lft_inw==40 & Inwoners1950$Geslacht=="Man" & Inwoners1950$Gemeente_recent<15,])
  table(Inwoners1950$Jaar) # eerst 192 records per jaar, vanaf 1978 65000 = 100 * 2 * 358
  table(Inwoners1950$Jaar, Inwoners1950$Provincie)
  
Inwoners1950_recent <- Inwoners1950  %>%
  group_by(Jaar, Geslacht, lft_inw, Gemeente_recent, Gemeente_naam_recent, Provincie, Politieregio, Eenheid) %>%
  summarize(Inwoners=sum(Inwoners), .groups='drop') %>% # 3079524
  arrange(Jaar, Provincie, Eenheid, Politieregio, Gemeente_recent, Gemeente_naam_recent, Geslacht, lft_inw) %>%
  select( Jaar, Provincie, Eenheid, Politieregio, Gemeente_recent, Gemeente_naam_recent, Geslacht, lft_inw, Inwoners)

  table(Inwoners1950_recent$Jaar) # eerst 192 records per jaar, vanaf 1978 65000 = 100 * 2 * 358
  table(Inwoners1950_recent$Jaar, Inwoners1950_recent$Provincie)
  
(series <- count(Inwoners1950_recent, Jaar) %>% # eerst 192 records per jaar, vanaf 1978 154000 afnemend
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
  

Inwoners_gemeente_historisch <- Inwoners1950 %>%
  group_by(Jaar, Provincie, Eenheid, Politieregio, Gemeente_nr, Gemeente_naam, Gemeente_recent, Gemeente_naam_recent) %>%
  summarize(Inwoners=sum(Inwoners), .groups='drop') # 25839

  count(Inwoners1950, Gemeente_naam_recent, wt=Inwoners)
  print(pivot_wider(count(Inwoners1950_recent, Jaar, Provincie, wt=Inwoners), names_from=Provincie, values_from=n), n=10000)
  print(pivot_wider(count(Inwoners1950_recent, Jaar, Eenheid, wt=Inwoners), names_from=Eenheid, values_from=n), n=10000)

Inwoners_1950_recent <-  left_join(Inwoners1950_recent, series2 %>% select(-n, -cum, -bestandsgrootte), by = join_by(Jaar)) %>%
  select(-minJ, -maxJ)
  
  print(pivot_wider(count(Inwoners_1950_recent, Jaar, Provincie, wt=Inwoners), names_from=Provincie, values_from=n), n=10000)
  print(pivot_wider(count(Inwoners_1950_recent, Jaar, Eenheid, wt=Inwoners), names_from=Eenheid, values_from=n), n=10000)
  
  rm(Inwoners_cbs, Inwoners1950, Inwoners1950_recent)
  gc()
  
# #Schrijf naar ods
# #NIEUW bestand
# write_ods(Inwoners_gemeente_historisch,
#           path=file.path("//HERA/KISS/Qlik/ods","Inwoners_gemeente.ods"),
#           sheet = "Gemeente_historisch")
# 
# # AANVULLEN IN BESTAAND BESTAND
# write_ods(Inwoners_1950_recent %>% filter(serie==4) %>% select(-serie, -Provincie,-Eenheid, -Politieregio, -Gemeente_recent, -Gemeente_naam_recent),
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
  openxlsx::writeData(wb,    "Inwoners_1950_1977", Inwoners_1950_recent %>% filter(serie==4) %>% select(-serie, -Provincie,-Eenheid, -Politieregio, -Gemeente_recent, -Gemeente_naam_recent))
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

# ToDo:
#   Omschrijvingen van Provincie, Polreg, Eenheid
#   Leeftijdsklassen (welke precies?)

#write.xlsx(Inwoners_gemeente_historisch, file=file.path("//HERA/KISS/Qlik/ods","Inwoners_gemeente_historisch.xlsx"))
#write.csv2(Inwoners1950_oms[Inwoners1950_oms$Jaar<2000,], file=file.path("//HERA/KISS/Qlik/ods","Inwoners_1950_1999.csv"), row.names=FALSE)

#Gehele bestand (recent)
names(Inwoners_1950_recent)
write.csv2(Inwoners_1950_recent %>% select(-serie), file=file.path("//HERA/KISS/Qlik/ods","Inwoners_gemeente_recent.csv"), row.names=FALSE)

#############################################################################################################




