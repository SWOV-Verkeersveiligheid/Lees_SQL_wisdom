#############################################################################################################
# lees_SQL_voor_ods.R
#
#
# versie wie    wat
# 0.1    Niels  dbReadTable
# 0.2    Niels  dbGetQuery, export van Verkeersdoden vanaf 1950
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
          
