#install required packages
if(!require(pacman))install.packages("pacman")
pacman::p_load('rvest', 'stringi', 'dplyr', 'tidyr', 'measurements', 'reshape2','foreach','doParallel','raster','curl','httr','Iso','data.table','RJSONIO')

conflict_prefer('filter','dplyr')
conflict_prefer('select','dplyr')
conflict_prefer("mutate", "dplyr")
conflict_prefer("rename", "dplyr")
conflict_prefer("melt", "reshape2")

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
clusterExport(cluster,c("%>%","html_nodes","read_html","PGA_url","Categories","html_attr","mutate","rename","html_text","stri_sub","Statistics"))
clusterEvalQ(cluster,"stat_link")
system.time(
  stat_links_par<-parLapply(cluster,1:nrow(Categories),stat_link))
stopCluster(cluster)

#combining a list of data frames generated above by the parallel computation
statstics_links<-stat_links_par%>%bind_rows(.id = "column_label")


#extract values 
values=read_html(paste0(PGA_url,'/stats/stat.02569.y2019.eoff.t016',".html"))%>%
  html_nodes("option")%>%
  html_attrs()%>%unlist()%>%
  data.frame()%>%rename(values=".")%>%
  filter(values!="selected")%>%
  mutate(Detail=read_html(paste0(PGA_url,'/stats/stat.02569.y2019.eoff.t016',".html"))%>%
           html_nodes("option")%>%
           html_text())

#split the data frame Value based on their option type
Season<-values%>%slice(3:12)
time_period<-values%>%slice(19:20)
tournament_list<-values%>%slice(21:n())

#concatenating strings to make up url links to the stat tables
stats_link_with_dot<-mapply(function(link) paste0(PGA_url,link,"."),statstics_links$Stats)%>%
  data.frame()

#replicating the statistics_links data frames to make the data frame as same length with stats_links*seasons
replicated_data <- replicate(10, statstics_links, simplify = FALSE)%>%bind_rows()

#concatenate links with seasons 
link_with_season<-mapply(function(link,season) paste0(link,season,"."),stats_link_with_dot,Season$values)%>%
  data.frame()%>%
  unlist()%>%
  data.frame()

#combine the stats links with the replicated stats data
data_frame_with_cat<-cbind(replicated_data,tables_links=unlist(link_with_season))

#concatenate links with time period
tables_links<-mapply(function(link,t_period) paste0(link,t_period,"."),data_frame_with_cat$tables_links,time_period$values[2])%>%data.frame()%>%
  unlist()%>%
  data.frame()

#concatenate links with tournaments
#tables_links<-mapply(function(link,tournament) paste0(link,tournament,".html"),tables_links$.,tournament_list$values[20])%>%
#    data.frame()%>%
#   unlist()%>%
#   data.frame()


tables_links<-lapply(tournament_list$values, function(x) {
  strings_concat <- paste0(tables_links$., x,".html")})
  
for (i in 1:length(tournament_list[1]))
{
  #combine data with category with the corresponding links
  final_data<-cbind(replicated_data,Links=tables_links[[i]])


#function that extracts tables from each link, prompting the system to sleep for ten seconds if running into DDOS issues while scraping tables
extract_tables<-function(links)
{
  repeat
  {
    res<-try(tables<-read_html(final_data$Links[links])%>%html_nodes("table")%>%html_table()%>%.[2]%>%data.frame())
    if(inherits(res,"try-error"))
    {
      Sys.sleep(10)
    }
    else
    {
      break
    }
  }
  return(tables)
  
}


#setup parallel backend to use many processors to extract all tables from url links above (link_to_stats_tables)
cores=detectCores()
cl <- makeCluster(cores)
clusterExport(cl,c("%>%","read_html","final_data","html_nodes","html_table","extract_tables"))
clusterEvalQ(cl,c("%>%","read_html","final_data","html_nodes","html_table","extract_tables"))
system.time(alltables<-parLapply(cl,1:nrow(final_data),extract_tables))
stopCluster(cl)

#remove data frames with empty rows
alltables_emptyremoved<-alltables[sapply(alltables, function(i)dim(i)[1])>0]
all_links_emptyremoved<-final_data[sapply(alltables, function(i)dim(i)[1])>0,]

#changing the structure of data frames and include the corresponding category, activities and links
combined_tables<-function(table_no)
{
  if("PLAYER.NAME" %in% colnames(as.data.frame(alltables_emptyremoved[table_no])))
  {
    alltables_outlinks<-alltables_emptyremoved[[table_no]]%>%data.frame() %>% melt(id.vars=c("PLAYER.NAME"),
                                                                                   measure.vars = names(alltables_emptyremoved[[table_no]][,-which(names(alltables_emptyremoved[[table_no]]) == "PLAYER.NAME")]))
    alltables_withlinks<-alltables_outlinks%>%mutate(Links=rep(all_links_emptyremoved$Links[table_no],nrow(alltables_outlinks)),
                                                     Category=rep(all_links_emptyremoved$Category[table_no],nrow(alltables_outlinks)),
                                                     Activities=rep(all_links_emptyremoved$Activities[table_no],nrow(alltables_outlinks)))
  }
  else
    return(NULL)
  
  return(alltables_withlinks)           
}


combined_tables<-lapply(1:length(alltables_emptyremoved), combined_tables)

#turn the list of data frames into one big data frame
combined_tables_final<-do.call("rbind", combined_tables)

#split the links into season, time_period and tournaments using regex
split_links<-stringr::str_match(combined_tables_final$Links, '.*\\.(y\\d+)\\.(\\w+)\\.(t\\d+)')
colnames(split_links)<-c("Links","Seasons","T_period","Tournament")

#combine the split data frame with the corresponding stats
Master_data_final<-cbind(combined_tables_final,split_links[,2:4])

#turn the master data into json format
Master_data_final_json<-toJSON(Master_data_final)

#save master data as a json file
write.csv(Master_data_final,paste0("pga_final_data_",
                                    tournament_list$values[i],".csv",sep=""))
  

}


write.csv(tournament_list,'tournament_data.csv')























