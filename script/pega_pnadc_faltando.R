library(tidyverse)

crossing(c(2023,2024), 1:4) %>% slice(1:6)

for (i in 1:8) {
  PNADcIBGE::get_pnadc(crossing(c(2023,2024), 1:4)[i,1] %>% pull(), quarter = crossing(c(2023,2024), 1:4)[i,2] %>% pull(), labels = FALSE,deflator = FALSE,design = FALSE) %>% 
    arrow::write_parquet(., paste0("C:\\Users\\Pedro Gesteira\\Documents\\pnadc\\trim", "\\", "PNADC_0",crossing(c(2023,2024), 1:4)[i,2] %>% pull(),crossing(c(2023,2024), 1:4)[i,1] %>% pull(), ".parquet"))    
}