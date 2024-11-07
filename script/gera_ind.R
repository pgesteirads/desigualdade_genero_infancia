library(tidyverse)


#As pnadcs tão no seguinte caminho

path_pnadc <- "C:\\Users\\Pedro Gesteira\\Documents\\pnadc\\trim"



arrow::read_parquet(list.files(path_pnadc, full.names = TRUE)[1])

crossing(c(2023,2024), 1:4) %>% slice(1:6)

for (i in 1:8) {
PNADcIBGE::get_pnadc(crossing(c(2023,2024), 1:4)[i,1] %>% pull(), quarter = crossing(c(2023,2024), 1:4)[i,2] %>% pull(), labels = FALSE,deflator = FALSE,design = FALSE) %>% 
arrow::write_parquet(., paste0("C:\\Users\\Pedro Gesteira\\Documents\\pnadc\\trim", "\\", "PNADC_0",crossing(c(2023,2024), 1:4)[i,2] %>% pull(),crossing(c(2023,2024), 1:4)[i,1] %>% pull(), ".parquet"))    
}

list.files(path_pnadc, full.names = TRUE, pattern = "parquet")

lista_tabelas <- 
map(list.files(path_pnadc, full.names = TRUE, pattern = "parquet"), function(x) {
  
  arrow::read_parquet(x, col_select = c("Ano","Trimestre","Capital","V1028", "V2007", "V2009", "V2010", "V4039","VD4001", "VD4002","VD4005")) %>% 
    filter(Capital == "33")
  
})

base_pnadc <- list_rbind(lista_tabelas)

base_pnadc %>% group_by(Ano,Trimestre) %>% summarise(mulheres_ocupadas = sum(V1028[which(V2007 == "2"  & VD4002 == "1")], na.rm = TRUE),
                                                     total_mulheres = sum(V1028[which(V2007 == "2" )], na.rm = TRUE)) %>% 
  mutate(perc_mulheres_ocupadas = mulheres_ocupadas/total_mulheres*100) %>% 
  writexl::write_xlsx(.,"produtos/tabelas/perc_pnadc_mulheres_ocupadas.xlsx")
