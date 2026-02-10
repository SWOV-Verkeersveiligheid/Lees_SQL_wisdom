#############################################################################################################
# lees_SQL metadata.R
#
#
# versie wie    wat
# 0.1    Niels  omzetten SAS bevraging naar R tb overzicht beschikbare data(bronnen)
#
#############################################################################################################
#
# Een organisatie levert meerdere keren een produkt, waarvan nul, een of meerdere datamatrixen worden gemaakt (die weer velden en codeboeken hebben)
# We rapporteren per produkt wat de leverende organisatie is, eerste/laatste levering, eeste/laatste jaar in de datamatrix, aantal rijen.kolommen
#
#rm(list=ls())
.libPaths() # 

library(dplyr)
library(tidyr)
#library(openxlsx)
#library(readODS)
library(DBI)          # Database interface
require(foreach)
#library(data.table)

# lees- en schrijfrechten op de wisdom DB in Uranus server

update_DB <- FALSE
source("//hera/kiss/restricted/AssignWisdom_Mod_uranus.R")


# Een organisatie heeft meerdere producten en gegevesbronnen. Daaruit worden weer meerdere matrixen gemaakt die elk een aantal variabelen hebben met bijbehorend codeboek.
# Een codeboek wordt gebruikt in meerdere vaiabelen die elk in meerdere matrixen gebruikt kunnen worden. Een matrix heeft soms meerdere bronnen.

WisdomTabellen_dbo   <- as.data.frame(odbc::dbListTables(conUW, catalog='Wisdom', schema='dbo')) # 39
WisdomTabellen_admin <- as.data.frame(odbc::dbListTables(conUW, catalog='Wisdom', schema='Admin1')) # 521
names(WisdomTabellen_dbo) <- "tabelnaam"
names(WisdomTabellen_admin) <- "tabelnaam"
WisdomTabellen_dbo$schema <- "dbo"
WisdomTabellen_admin$schema <- "Admin1"
WisdomTabellen <- rbind(WisdomTabellen_admin, WisdomTabellen_dbo) %>%
  mutate(dmx_naam=toupper(tabelnaam))

rm(WisdomTabellen_admin, WisdomTabellen_dbo)

#dbListTables(conUW, schema="sys")
#dbListFields(conUW, SQL("dbo.datamatrix"))

dmx <-  dbGetQuery(conUW, "SELECT * FROM wisdom.dbo.datamatrix") %>% # 217
  select(-dmx_orderby, -dmx_ascii_source, -dmx_afhvar, -sta_nr, -dmx_bndgebruik, -dmx_dst1, -dmx_dst2, -dmx_script, 
         -hlp_nr_inh, -hlp_nr_dst, -dmx_dicnr, -dmx_teller, -dmx_noemer, -dmx_factor, -dmx_afhvargb, -dmx_transpose, -dmx_blob, -dmx_whereclause, -dmx_transposeGB) %>%
  mutate(dmx_datum=as.Date(dmx_datum),
         dmx_naam=toupper(dmx_naam))
  #-dmx_pasop_nl, 

systables <-  dbGetQuery(conUW, "select * from sys.tables") %>% # 441 - 1
  select(schema_id, dmx_naam=name, modify_date, type_desc) %>%
  mutate(modify_date=as.Date(modify_date),
         Udmx_naam=toupper(dmx_naam),
         schema_naam=case_when(
           schema_id==1 ~ "dbo",
           schema_id==5 ~ "Admin1",
           schema_id==6 ~ "temp")) %>%
  filter(schema_id!=6)

sysviews <-  dbGetQuery(conUW, "select * from sys.views") %>% # 120
  select(schema_id, dmx_naam=name, modify_date, type_desc) %>%
  mutate(modify_date=as.Date(modify_date),
         Udmx_naam=toupper(dmx_naam),
         schema_naam=case_when(
           schema_id==1 ~ "dbo",
           schema_id==5 ~ "Admin1",
           schema_id==6 ~ "temp")) %>%
  filter(schema_id!=6)


#Rob heeft Views gemaakt op de sys.tables, met ook alle variabelen erin
#VIEW show_table_content in SQL
# SELECT  schema_name(t2.schema_id) AS schema_name, t2.name AS table_name, t2.create_date, t2.modify_date, 
#         x.[Column Name], x.[Data type], x.var_nr AS codeboek_var_nr, x.[Max Length], x.precision, x.scale, x.is_nullable, x.[Primary Key]
# FROM    sys.tables AS t2 LEFT OUTER JOIN
# (SELECT c.object_id, c.name AS [Column Name], t.name AS [Data type], v.var_nr, c.max_length AS [Max Length], c.precision, c.scale, c.is_nullable, ISNULL(i.is_primary_key, 0) AS [Primary Key]
#   FROM  sys.columns AS c LEFT OUTER JOIN
#   dbo.variabele AS v ON UPPER(c.name) = UPPER(v.var_naam) INNER JOIN
#   sys.types AS t ON c.user_type_id = t.user_type_id LEFT OUTER JOIN
#   sys.index_columns AS ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id LEFT OUTER JOIN
#   sys.indexes AS i ON ic.object_id = i.object_id AND ic.index_id = i.index_id) AS x ON t2.object_id = x.object_id
# WHERE   (schema_name(t2.schema_id) = 'Admin1')

# table_content <-  dbGetQuery(conUW, "select * from show_table_content") %>%
#   group_by(dmx_naam=table_name, modify_date) %>%
#   summarize(kolommen=n(), .groups='drop') %>%
#   mutate(modify_date=as.Date(modify_date))

rm(tabel, rijen, kolommen, minY, maxY, velden, tabel_index)

#beide <- toupper(dmx$dmx_naam) %in% toupper(table_content$dmx_naam) #30
beide <- dmx$dmx_naam %in% systables$Udmx_naam #217
dmx[beide==FALSE,c(1,2)] # 28 stuks
systables[systables$Udmx_naam != systables$dmx_naam, c(2,4,5)]

extra_WisdomTabellen_nr <- WisdomTabellen$dmx_naam %in% systables$Udmx_naam #560
extra_WisdomTabellen <- WisdomTabellen[! extra_WisdomTabellen_nr,] #120
extra_WisdomTabellen_vw  <- extra_WisdomTabellen$dmx_naam %in% sysviews$Udmx_naam # 120 views
# incl views en view-REF tabellen. Waarom zitten die er standaard niet bij dan?

# 4 Views
# 42  RISICO_SLACHTOFFER_AFSTAND 6085690        9    NA   NA
# 43   RISICO_SLACHTOFFER_LETSEL  883121       10    NA   NA
# 112  RISICO_BESTUURDER_AFSTAND 4666987       18  1995 2023
# 132          dbo.LMR_SELECTIE2       0       17    NA   NA
# en een aantal lege 'tabellen'

# er vallen er ook nog een paar uit, zoals
# dbo.GEMEENTEN_MUTATIES
# Zijn die in wisheer expliciet uitgesloten van de dbo.datamatrix?

tabelprops <- foreach (tabel_index = 1 : (nrow(systables)),
               .combine = bind_rows,
               .multicombine = TRUE) %do% {

  #tabel <- dmx[tabel_index,]$dmx_naam
  #tabel <- table_content[tabel_index,]$dmx_naam
  #dmx_naam <- "BRON_VOERTUIGSTATUS"
  dmx_naam <- systables[tabel_index,]$Udmx_naam
  schema <- systables[tabel_index,]$schema_naam
  
  #tabelprop <- dbGetQuery(conUW, paste0("SELECT * FROM ",dmx_naam))
  # rijen <- NA_integer_
  # kolommen <- 0
  minY <- NA_integer_
  maxY <- NA_integer_
#  cat(tabel_index,  schema, dmx_naam, "\n")  
  # gaat fout als het een risicotabel betreft (incl BAG en Registratiegraad?) en als er een - teken in de naam zit
  if (! dmx_naam %in% c("RIJBEWIJS-RATIO", "DODEN-VOERTUIGKM", "RISICO_SLACHTOFFERS", "RIJBEWIJS_B_NOVG", 
                     "RIJBEWIJS_NOVG_B_POP", "RIJBEWIJS_A_NOVG", "RIJBEWIJS_NOVG_A_POP",
                     "SLACHTOFFERS_INWONERS", "BRON_WEGVAKGEOGRAFIE", "GIS_LIMBURG", "GIS_TDN",
                     "RISICO_BESTUURDERS", "RISICO_BESTUURDER_LETSEL", "RISICO_BESTUURDERS",
                     "CBS_DOODSOORZ", "IRTAD", "ACTUELE_WEGENLIJST", "WERKELIJK_GEWOND_INWONERS",
                     "IVO_OVG_VERVOER", "POLITIE", "TEST", "CBS_DOODSOORZ2", "CBS_DOODSOORZ3",
                     "BRON_OBJECT_COPY", "BRON_ONGEVAL_COPY")) {
#    dmx_naam <- "CODEBOEK"
#    schema <- "dbo"
    velden <- toupper(DBI::dbListFields(conUW, DBI::Id(schema, table = dmx_naam)))
    #velden <- odbc::dbListFields(conn=conUW, name=dmx_naam, schema_name=schema)
 
    
    kolommen <- length(velden)
    if ("ROWID" %in% velden) {kolommen <- kolommen-1}
    if ("JAAR" %in% velden) {
      # minY <- as.integer(odbc::dbGetQuery(DBI::sqlInterpolate(conUW,
      #                     "SELECT min(JAAR) FROM ?id",
      #                     id = DBI::Id(schema, dmx_naam))))
      minY <- as.integer(dbGetQuery(conUW, paste0("SELECT min(JAAR) FROM [", schema, "].[", dmx_naam, "]")))
      maxY <- as.integer(dbGetQuery(conUW, paste0("SELECT max(JAAR) FROM [", schema, "].[", dmx_naam, "]")))
    } else if ("BEGDAT" %in% velden & "ENDDAT" %in% velden) {
        minY <- dbGetQuery(conUW, paste0("SELECT min(BEGDAT) FROM [", schema, "].[", dmx_naam, "]"))
        minY <- as.integer(format(minY,'%Y'))
        maxY1 <- dbGetQuery(conUW, paste0("SELECT max(BEGDAT) FROM [", schema, "].[", dmx_naam, "]"))
        maxY2 <- dbGetQuery(conUW, paste0("SELECT max(ENDDAT) FROM [", schema, "].[", dmx_naam, "]"))
        a <- setNames(rbind(maxY1, maxY2), "datum")
        maxY <- as.integer(format( max(a$datum),'%Y'))
      } else if ("WVK_BEGDAT" %in% velden & "WVK_EINDDAT" %in% velden) {
        minY <- dbGetQuery(conUW, paste0("SELECT min(WVK_BEGDAT) FROM [", schema, "].[", dmx_naam, "]"))
        minY <- as.integer(format(minY,'%Y'))
        maxY1 <- dbGetQuery(conUW, paste0("SELECT max(WVK_BEGDAT) FROM [", schema, "].[", dmx_naam, "]"))
        maxY2 <- dbGetQuery(conUW, paste0("SELECT max(WVK_EINDDAT) FROM [", schema, "].[", dmx_naam, "]"))
        a <- setNames(rbind(maxY1, maxY2), "datum")
        maxY <- as.integer(format( max(a$datum),'%Y'))
      } 
    #else {cat("geen jaar in ", dmx_naam, velden, "\n")}
    if (dmx_naam=="SARTRE_GORDEL") {
      minY <- as.integer(1992) # SARTRE 1: 1991-1992, SARTRE 2: 1996-1997
      maxY <- as.integer(2003) # SARTRE 3: 2002-2003, SARTRE 4: 2009-2012 
    }
    # -2000 is een doel of langjarig gemiddelde
    if (dmx_naam %in% c("BAG_REGIO")  & minY == -2000) {minY <- 1970}
    if (dmx_naam %in% c("BAG_POLREG") & minY == -2000) {minY <- 1993}
    if (dmx_naam %in% c("WEER")       & minY == -2000) {minY <- 1985}
    
    rijen <- as.integer(dbGetQuery(conUW, paste0("SELECT count(*) FROM [", schema, "].[", dmx_naam, "]")))
    # zonder escape cat(paste0('SELECT count(*) FROM "', schema,".", dmx_naam, '"'))
  }

  df <- as.data.frame(cbind(schema, dmx_naam, rijen, kolommen, minY, maxY)) %>%
    mutate(rijen=as.integer(rijen), kolommen=as.integer(kolommen),
           minY=as.integer(minY),   maxY=as.integer(maxY))
} #440

# dif <- left_join(tabelprops, tabelprops2, by = join_by(schema, dmx_naam, rijen, kolommen)) %>%
#   filter(is.na(minY.x) | is.na(maxY.x))

tabellen <- left_join(systables, tabelprops, by = c("Udmx_naam"="dmx_naam", "schema_naam"="schema")) %>%
  select(-schema_id) %>%
  full_join(dmx, by= join_by(dmx_naam)) %>%
  mutate(Udmx_naam=toupper(dmx_naam)) #480

table(tabellen$dmx_type, useNA= 'ifany') # type=2 is een Transformatietabel
count(tabellen[tabellen$dmx_type %in% c(1,2,3,4), ], dmx_type, dmx_naam)
# 1 Risico (teller/noemer)
# 2 Transformatietabel
# 3 externe bestanden
# 4 View

# tabelprops[tabelprops$dmx_naam=="CODEBOEK",]
# systables[systables$Udmx_naam=="CODEBOEK",]
# tabellen[tabellen$Udmx_naam=="CODEBOEK",]

gegbron <- dbGetQuery(conUW, "SELECT * FROM wisdom.dbo.gegevensbron") #186
# unique(gegbron$dmx_nr) %in% unique(dmx$dmx_nr) # ze komen allemaal voor
# unique(dmx$dmx_nr) %in% unique(gegbron$dmx_nr) # sommige tabellen hebben geen product

# Tabellen die gevoed worden door meer dan 1 gegevensbron (reg%, risico) komen onder beide bronnen voor.
geg_dmx <- full_join(gegbron, tabellen, by=join_by(dmx_nr)) #522
# geen_details  <- geg_dmx[is.na(geg_dmx$dmx_naam),]$dmx_nr
# dmx[dmx$dmx_nr %in% geen_details,c(1,2,5,3)]

# levering <- dbGetQuery(conUW, "SELECT * FROM wisdom.dbo.levering") %>%
#   filter(prod_nr==62)
  
levering <- dbGetQuery(conUW, "SELECT * FROM wisdom.dbo.levering") %>%
  filter(!is.na(prod_nr)) %>%
  arrange(prod_nr, lever_datum) %>%
  group_by(prod_nr) %>%
  summarise(EersteLevering     = min(lever_datum, na.rm=T),
            LaatsteLevering    = max(lever_datum, na.rm=T),
            lever_omschrijving = last(lever_omschrijving),
            lever_archief      = last(lever_archief),
            p_min              = min(lever_periode, na.rm=T),
            p_max              = max(lever_periode, na.rm=T),
            lever_voorwaarde   = last(lever_voorwaarde),
            lever_bewaartermijn= last(lever_bewaartermijn),
            lever_IBklasse     = max(lever_IB_klasse, na.rm=T),
            lever_IBopvolging  = max(lever_IB_opvolging, na.rm=T),
            lever_aantal  = n()) %>%
  mutate(lever_perioden=case_when(
    is.na(p_min) ~ NA_character_,
    TRUE ~ paste(p_min, p_max, sep=" tm "))) %>%
  mutate(lever_IBklasse=ifelse(lever_IBklasse== -Inf, NA_integer_, lever_IBklasse)) %>%
  select(-p_min, -p_max) #70
  
count(levering, prod_nr, lever_aantal)
count(levering[!is.na(levering$lever_perioden) | !is.na(levering$lever_IBklasse),], prod_nr, lever_IBklasse, lever_perioden)


produkt <- dbGetQuery(conUW, "SELECT * FROM wisdom.dbo.produkt") # 77
prod_geg_dmx <- dbGetQuery(conUW, "SELECT * FROM wisdom.dbo.produkt") %>%
  select(-prod_persoon, -prod_telefoon, -prod_fax, -prod_email, -prod_help, -prod_url, -prod_kb) %>%
  #select(-prod_omschrijving, -prod_memo) %>%
  mutate(Frequentie=case_when(
    prod_update== 0 ~ "informatie",
    prod_update== -1 ~ "stopgezet",
    between(prod_update,30,190) ~ "semester",
    between(prod_update,300,370) ~ "jaarlijks",
    prod_update >370 ~ "na meerdere jaren",
    TRUE ~ "anders")) %>%
  left_join(levering, by = join_by(prod_nr)) %>%
  full_join(geg_dmx, by = join_by(prod_nr)) # 561

table(prod_geg_dmx$prod_update, prod_geg_dmx$Frequentie, useNA='ifany')
prod_geg_dmx[which(prod_geg_dmx$prod_update==0),c(3,4,10,12)] # informatie
table(prod_geg_dmx$minY, prod_geg_dmx$maxY, useNA='ifany')


Prod_org_geg_dmx <- dbGetQuery(conUW, "SELECT * FROM wisdom.dbo.organisatie") %>%
  select(-org_naam_lang, -org_straat, -org_postbus, -org_postcode, -org_plaats, -org_telefoon, -org_fax, -org_email, -org_url, -org_naam_langgb) %>%
  full_join(prod_geg_dmx, by = join_by(org_nr)) %>%
  mutate(Periode=paste(minY, maxY, sep="-")) %>%
  mutate(Periode=ifelse(Periode=="NA-NA","",Periode)) %>%
  select(-minY, -maxY) %>%
  arrange(org_naam_kort, dmx_naam, prod_naam) %>%
  filter(! grepl("TRANS_", dmx_naam)) %>%
  filter(! substr(dmx_naam,1,2) %in% c("T_")) %>% # er zijn 8 T_ tabellen die niet in dbo.datamatrix voorkomen
  filter(! dmx_type %in% c(2)) %>% # anders laat hij NA ook weg?
  select(-dmx_type) %>%
  filter(is.na(dmx_naam) | schema_naam!="dbo" | dmx_naam==Udmx_naam) %>% # pas op als dmx_naam leeg is
  filter(! grepl("REF_", dmx_naam)) %>%
  filter(! grepl("RUW_", dmx_naam)) %>%
  filter(! grepl("RAW_", dmx_naam)) %>%
  filter(! grepl("BCK", toupper(dmx_naam))) %>%
  filter(! grepl("BACK", toupper(dmx_naam))) %>%
  filter(! grepl("TEST", dmx_naam)) %>%
  filter(! grepl("COPY", dmx_naam)) %>%
  filter(! grepl("_OUD", toupper(dmx_naam))) %>%
  filter(! grepl("_OLD", toupper(dmx_naam))) %>%
  filter(! grepl("OLD_", toupper(dmx_naam))) %>%
  filter(! grepl("NB01_", toupper(dmx_naam))) %>%
  filter(! grepl("_2017", dmx_naam)) %>%
  filter(! grepl("_2018", dmx_naam)) %>%
  filter(! grepl("_TableColumnsUsed", dmx_naam)) %>% #295
  mutate(lever_aantal=ifelse(is.na(lever_aantal),0, lever_aantal)) %>%
  mutate(Internat=case_when(
    org_naam_kort %in% c("ETSC", "European Union", "Eurostat", "FERSI", "IHME", "IRF", "ITF", "NHTSA", "VIAS","WHO") ~ 1,
    TRUE ~ 0)) %>%
  arrange(Internat, org_naam_kort, prod_naam, dmx_naam) %>%
  #  select(-Udmx_naam, -prod_update) %>% 
  select(Product=prod_naam, Bronhouder=org_naam_kort, Prod_naamswov=prod_beheerder_swov, prod_docu, prod_memo, Frequentie, lever_aantal,
         LaatsteLevering, lever_voorwaarde, lever_IBklasse, lever_IBopvolging,
         Tabel=dmx_naam, Periode, TabelOms=dmx_omschrijving, rijen, kolommen, TabelPasop=dmx_pasop_nl)


print(Prod_org_geg_dmx[which(Prod_org_geg_dmx$schema_naam=="dbo" & Prod_org_geg_dmx$Udmx_naam!=Prod_org_geg_dmx$Tabel), c(1,2,4)]) #0
#  print(Prod_org_geg_dmx[which(Prod_org_geg_dmx$prod_naam=="WEGGEG"),])
#  print(Prod_org_geg_dmx[which(Prod_org_geg_dmx$Product=="WEGGEG"),])
#print(Prod_org_geg_dmx[which(Prod_org_geg_dmx$Product=="Bevolking"),])
table(Prod_org_geg_dmx$lever_aantal, useNA='ifany')

names(Prod_org_geg_dmx) # org_nr org_naam_kort Internat
                        # prod_nr prod_naam prod_omschrijving prod_update prod_docu prod_memo prod_beheerder_swov Frequentie
                        # lever_aantal EersteLevering LaatsteLevering lever_omschrijving lever_archief lever_voorwaarde lever_bewaartermijn lever_IBklasse lever_IBopvolging
                        # dmx_nr dmx_naam modify_date type_desc Udmx_naam schema_naam rijen kolommen Periode dmx_omschrijving dmx_pasop_nl dmx_datum dmx_omschrijvinggb
            

getwd()
write.csv2(Prod_org_geg_dmx, file.path("//hera/kiss//Qlik/ods/Metadata","ProductBronTabel.csv"), row.names=FALSE)

#########################################################################################################################

