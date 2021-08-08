#install required packages
if(!require(pacman))install.packages("pacman")
pacman::p_load('rvest', 'stringi', 'dplyr', 'tidyr', 'measurements', 'reshape2','foreach','doParallel','raster','curl','httr','Iso','data.table','RJSONIO')

#setting proxy server


#set up the link to PGA website
PGA_url <- "https://www.pgatour.com"
base_url <- "https://www.pgatour.com/stats/"

#extract URL for categories (OFF THE TEE,APROACH THE GREEN,AROUND THE GREEN,PUTTING, SCORING, STREAKS, MONEY/FINISHES, POINTS/RANKINGS)
pga_web=read_html(paste0(PGA_url,'/stats.html'))
Categories <- pga_web%>%html_nodes('.navigation.section')%>%html_nodes("a")%>%html_attr("href") %>% data.frame() %>%
  mutate(Name = html_nodes(read_html("https://www.pgatour.com/stats.html"), '.navigation.section') %>% 
           html_nodes("a") %>% 
           html_text()
  ) %>%
  rename(Link = names(.)[1]) %>% 
  slice(2:(n()-1))

#Create an empty data frame to store the url of stats numbers along with the activity and category name
Statistics <- data.frame("Stats"     = NA,
                         "Activity"     = NA,
                         "Category" = NA)


#Extract the url for stat numbers under each category and append in Statistics data frame
stat_link<-function(cat)
{
  Statistics_i <- html_nodes(read_html(paste0(PGA_url, Categories[cat,1])), "[class='table-content clearfix']") %>% 
    html_nodes("a")   %>% 
    html_attr("href") %>%
    data.frame()      %>%
    mutate(Activities = html_nodes(read_html(paste0(PGA_url, Categories[cat,1])), "[class='table-content clearfix']") %>% 
             html_nodes("a") %>% 
             html_text()
    ) %>% 
    rename(Stats = names(.)[1]) %>%
    mutate(Stats = stri_sub(Stats, 1, -6),
           Category = Categories$Name[Categories$Link==Categories[cat,1]])
  
  Statistics <- rbind(na.omit(Statistics), Statistics_i)
  return(Statistics)
}

#setup parallel backend to use many processors
core=detectCores(1)
cluster=makeCluster(core[1])
registerDoParallel(cluster)
clusterExport(cluster,c("%>%","html_nodes","read_html","PGA_url","Categories","html_attr","mutate","rename","html_text","stri_sub","Statistics"))
clusterEvalQ(cluster,"stat_link")
system.time(
  stat_links_par<-parLapply(cluster,1:nrow(Categories),stat_link))
stopCluster(cluster)

#combining a list of data frames generated above by the parallel computation
statstics_links<-stat_links_par%>%bind_rows(.id = "column_label")

#extract values 
values=read_html(paste0(PGA_url,statstics_links[1,2],".html"))%>%
  html_nodes("option")%>%
  html_attrs()%>%unlist()%>%
  data.frame()%>%rename(values=".")%>%
  filter(values!="selected")%>%
  mutate(Detail=read_html(paste0(PGA_url,statstics_links[1,2],".html"))%>%
           html_nodes("option")%>%
           html_text())

Season<-values%>%slice(2:12)
time_period<-values%>%slice(18:19)
tournament_list<-values%>%slice(20:n())


#concatenating strings to make up url links to the stat tables
stats_link_with_dot<-mapply(function(link) paste0(PGA_url,link,".html"),statstics_links$Stats)%>%
  data.frame()


#extract table description


#function that extracts tables descriptions from each link, prompting the system to sleep for ten seconds if running into DDOS issues while scraping tables
  
  extract_table_descriptions<-function(links)
  {
      repeat
      {
        res<-try(tab_desc<-read_html(stats_link_with_dot$.[links])%>%html_nodes("[class='content-footer']")%>%html_nodes('p')%>%html_text()%>%data.frame())
        if(inherits(res,"try-error"))
        {
          Sys.sleep(10)
        }
        else
        {
          break
        }
      }
    return(tab_desc)

  }
  
  
  #setup parallel backend to use many processors to extract all tables from url links above (final_data$links)
  cores=detectCores()
  cl <- makeCluster(cores)
  clusterExport(cl,c("%>%","read_html","final_data","html_nodes","html_table","extract_table_descriptions","html_text"))
  system.time(allDesctiptions<-parLapply(cl,nrow(stats_link_with_dot),extract_table_descriptions))
  stopCluster(cl)
  
  #remove data frames with empty rows
  alltablesDesc_emptyremoved<-allDesctiptions[sapply(allDesctiptions, function(i)dim(i)[1])>0]%>%bind_rows()%>%rename(Desc=names(.)[1])
  all_links_emptyremoved<-stats_link_with_dot$.[sapply(allDesctiptions, function(i)dim(i)[1])>0]
  
  MasterData<-cbind(Desc=alltablesDesc_emptyremoved,Links=all_links_emptyremoved)

  #save master data as a json file
  write.csv(MasterData,"C:/Users/SurafelTilahun/Luma Analytics/Lumineers - Documents/5 Users/Surafel/Web Scraping/Table_desc_data/pga_final_tableDesc.csv",)
  


