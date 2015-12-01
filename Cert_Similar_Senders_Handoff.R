library(ggplot2)
library(shiny)
library(plyr)
library(lsa)
library(RColorBrewer)
library(reshape2)
new_color <- c((brewer.pal(5, "Set2")), (brewer.pal(6, "Set3")))
hive.connect <- function(){
  library('RODBC')
  hive <- odbcConnect("Hive")
  return(hive)
}
hive <- hive.connect()

#Get the aggregate level data to find similar senders:
#Note: there are already limits imposed on this data: 10 messages, 10 users and 5 days (for only Gmail and Yahoo)
oib_cert_all <- sqlQuery(hive, "select * from acase.oib_cert_ips_round2_aggregate_lim_no_state")

#seperate Cert vs. Non Cert:
cert_data <- subset(oib_cert_all, is.na(group_id) == FALSE)
non_cert_data <- subset(oib_cert_all, is.na(group_id) == TRUE)

colnames(cert_data)[2] <- c("isp_source")
colnames(non_cert_data)[2] <- c("isp_source")

cert_ip_vars <- subset_variables(cert_data)
non_cert_ip_vars <- subset_variables(non_cert_data)

#Put the full data set back together for later use:
original_df <- rbind(cert_data, non_cert_data)

#Process data in a loop to find all results for Cert Senders. Use Cosine similarity and a 0 to 1 scaling function
cert_df <- cert_ip_vars
non_cert_df <- non_cert_ip_vars

for(i in 1:dim(cert_df)[1]){
  #for length of the df, get results per IP, get top 50 results (or however many of interest)
  new_df <- rbind(cert_df[i,], non_cert_df)
  top.n <- 50
  scores <- get_similarity_scores_final(new_df)
  #getting the final results:
  results_final <- get_results_rank_single(scores, new_df$ip_address, top.n, original_df)
  if(i == 1) {
    cos_scale_df <- results_final
  } else {
    cos_scale_df <- rbind(cos_scale_df, results_final)
  }
}
save(cos_scale_df, file = "cos_scale_df_final.RData")


#*************************************************
#****  Getting data for Aggregate Results:  ******
#*************************************************
#bring in daily data for each source
#get averages

test_ip <- '103.20.18.163'



#get results from matching ip:
df1 <- subset(cos_scale_df, matching_ip == test_ip)
#get only the top.n results
results_final <- subset(df1, rank_val > (max(df1$rank_val) -( top.n + 1)))


#Final disply of results for *some* of the variables used for matching- also showing inbox and read rate (which were not used for matching)
#This is more of the behind the hood type of data- what pieces go into matching IPs
disp <- ddply(results_final, c("ip_address", "isp_source"), summarize,
              avg_inbox = mean(avg_inbox_rate),
              avg_rr = mean(avg_read_rate),
              vol = sum(total_all_msgs),
              users =sum(total_num_users),
              mean_subjects = mean(total_num_subjects),
              domains = max(total_num_domain),
              days = mean(num_days),
              variance = mean(std_inbox_rate),
              avg_msg_variance = mean(std_total_msgs),
              rr_variance = mean(std_read_rate))

#ordering the IPs by rank 
order_rank <- unique(subset(results_final, , c("ip_address", "rank_val")))
order_rank <- arrange(order_rank, desc(rank_val))

#Get 'average' for the result set:
avg_order <- data.frame(ip_address = "Average", rank_val = (min(order_rank$rank_val)-1))
order_rank <- rbind(order_rank, avg_order)
order_rank$ip_address <- factor(order_rank$ip_address, levels = unique(order_rank$ip_address), ordered = TRUE)

#get the average by source from other IPs:
match_disp <- subset(disp, ip_address != test_ip)
disp_Avg <- ddply(match_disp, c("isp_source"), summarize, 
                  mean(avg_inbox), 
                  mean(avg_rr), 
                  mean(vol), 
                  mean(users),
                  mean(mean_subjects),
                  mean(domains),
                  mean(days), 
                  mean(variance),
                  mean(avg_msg_variance),
                  mean(rr_variance))


disp_Avg$ip_address <- "Average"
colnames(disp_Avg) <- c("isp_source","avg_inbox", "avg_rr","vol","users" ,"mean_subjects", 
                        "domains" ,"days","variance","avg_msg_variance", "rr_variance","ip_address")

#Combine the original results with the calculated Average:
disp_df <- rbind(disp, disp_Avg)



#*************************************************
#******  Getting data for Daily Results:  ********
#*************************************************

#oib_cert_ips_data <- sqlQuery(hive, "select * from acase.oib_cert_ips_data")
#this can be pre-limited to just the IPS of interested, or just the ones with results

#subseting to only relevant results:
by_day_df <- subset(oib_cert_ips_data, ip_address %in% disp$ip_address)
#limiting the data to fields of interest, just so it's easier to manage:
avg_df <- subset(by_day_df, , c("ip_address", "source", "inbox_rate", "read_rate", "day", 
                                "sum_read", "total_inbox", "total_msgs", "num_users", "num_subjects"))


#now only getting IP addresses that are NOT the cert IP, in order to get an average:
avg_df2 <- subset(avg_df, ip_address != test_ip)
#avg_df2$overall_inbox <- avg_df2$total_inbox / avg_df2$total_msgs

avg_res <- ddply(avg_df2, c("source", "day"), summarize,
                 c = mean(inbox_rate),
                 a = sum(total_inbox),
                 b = sum(total_msgs),
                 d = mean(num_users),
                 e = mean(num_subjects),
                 f = mean(read_rate),
                 g = sum(sum_read))
avg_res$ip_address <- c("Average")
colnames(avg_res) <- c("source", "day",  "inbox_rate", 
                       "total_inbox", "total_msgs", "num_users", "num_subjects", "read_rate", "sum_read", "ip_address")           
avg_res$inbox_rate <- avg_res$total_inbox / avg_res$total_msgs
avg_res$read_rate <- avg_res$sum_read / avg_res$total_msgs

all_data_by_day <- rbind(avg_df, avg_res)

#************************************
#Final calculation to get daily inbox rate change
#This actually gets 2 measure of change- the day by change, 
#and the actual 'variance' (a statistical measure)
#See function below (in function list)
#************************************

all_data_by_day$inbox_rate <- all_data_by_day$inbox_rate * 100
colnames(all_data_by_day)[2] <- c("isp_source")
all_data_by_day$as_date <- as.Date(as.character(all_data_by_day$day), format= '%Y%m%d')
inbox_change <- daily_inbox_change(all_data_by_day)

#getting the overall numbers for daily inbox change:
inbox_change_calc <- ddply(inbox_change, c("ip_address", "isp_source"), summarize,
                  mean_diff = mean(diff, na.rm = TRUE),
                  variance = sd(inbox_rate, na.rm = TRUE))
inbox_rate_calc <- ddply(all_data_by_day, c("ip_address", "isp_source"), summarize,
                    avg_inbox_all = mean(inbox_rate, na.rm = TRUE))

average_numbers <- merge(inbox_change_calc, inbox_rate_calc, by = c("ip_address", "isp_source"))
#************************************



#*************************************************
#***** Some plots to double check the data  ******
#*************************************************
#df <- subset(disp_df, ,c("ip_address", "isp_source","avg_inbox", "avg_rr","vol","mean_subjects","domains","days", "variance"))

#Variables that went into the similarity score:
df<- subset(disp_df, ip_address %in% c(test_ip, 'Average'))
df <- unique(df)
df <- arrange(df, isp_source, ip_address)

eef <- melt(df, id.vars = c("ip_address", "isp_source"))
pp <- ggplot(eef, aes(x =isp_source, y = value,fill= ip_address ))
pp + geom_bar(stat = "identity", position="dodge")    + scale_fill_manual(values=new_color) +
  facet_wrap(~variable, scales="free_y") + labs(title = "Similarity Variables")


#Daily variables:
df2<- subset(all_data_by_day, ip_address %in% c(test_ip, 'Average'))
df2 <- subset(df2, , c("ip_address", "isp_source", "inbox_rate", "as_date"))
df2 <- unique(df2)
tt <- ggplot(df2, aes(x = as_date, y = inbox_rate,  color = ip_address)) 
tt + geom_line(size = 1.5) +
  scale_colour_manual(values = new_color) +
  facet_grid(~isp_source) +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) + labs(title = "Inbox Rate by Day")


#*************************************************
#***********   Functions    **********************
#*************************************************
subset_variables <- function(df){
  #colnames(df)[2] <- c("isp_source")
  si_index <-ddply(df, c("ip_address"), summarize,
                   srcs = length(unique(isp_source)),
                   domains_max = max(total_num_domain),
                   users =sum(total_num_users),
                   subjects_max = max(total_num_subjects),
                   dwoo = sum(total_sum_dwoo),
                 #  inbox = sum(total_sum_inbox),
                  # spam = sum(total_sum_spam),
                   trash = sum(total_sum_trash),
                   moved = sum(total_sum_moved),
                   read = sum(total_sum_read),
                   volume = sum(total_all_msgs),
                   avg_msg_variance = mean(std_total_msgs),
                   rr_variance = mean(std_read_rate))
                  # avg_inbox_rate = mean(avg_inbox_rate),
                  # avg_rr = mean(avg_read_rate))
  return(si_index)
}


get_similarity_scores_final <- function(df){
  #df is in original format, 
  #the first row HAS TO BE THE cert IP_DF, the rest is non cert
  #preprocessing for each df:
  df$ip_address <- NULL
  
  #removing inbox rate, since this is what we want to judge everything by
  df$avg_inbox_rate <- NULL
  
  dat <- data.frame(apply(df, MARGIN = 2, FUN = function(X) (X - min(X))/diff(range(X))))
  new_df <- data.frame(similarity_score_cosine_single(dat))
  
  return(new_df)
}


similarity_score_cosine_single <- function(df){
  #find the cosine similarity scores between a single ip 'df' and the entire df sent in
  #function(ip_df, df){ this was for 2 dfs, going back to 1
  # df_x <- rbind(ip_df, df)
  df_x <- df
  df_x <- as.matrix(df_x)
  x <- matrix(nrow =  dim(df)[1])
  #compare the first row to every other row:
  for(j in 1:dim(df_x)[1]){
    x[j] = cosine(df_x[1,], df_x[j,])
  }
  return(x)
}


get_results_rank_single <- function(df, ip_list, top.n, original_df){
  #This function will actually rank the results then add the matched IP with the results subset, get the ranked score, 
  # along with the rest of the entire data frame to use for plotting purposes. It works!
  #the 'df' here is the results from the other functions!!
  #this is for a single IP result, so it's a little different than the other function
  rank_df <- t(data.frame(rank(df[1])))
  colnames(rank_df) <- ip_list
  xc <- rank_df[1,]
  top_idx <- sort(xc, decreasing = TRUE)  
  top_ips <- attributes(top_idx[0:top.n+1])
  tmps <- data.frame(rank_val = top_idx[0:top.n+1])
  tmps2<- data.frame(ip_address = top_ips$names,
                     rank_val = tmps$rank_val)
  find_ips <- tmps2$ip_address
  tt <- subset(original_df,ip_address %in% find_ips)
  new_df<- merge(tt, tmps2, by = c("ip_address"))
  new_df$matching_ip <- top_ips$names[1]
  final_df <- new_df
  
  return(final_df)
}

rank_fun_single <- function(df){
  x <- matrix(nrow =  dim(df)[1])
  #for(i in 1:dim(df)[1] ) {
  x[1] = rank(df[1])  
  #}
  return(x)
}



daily_inbox_change <- function(df){
  #The goal is to create a function that outputs the daily change in inbox %
  change_df <- subset(df, , c("ip_address", "isp_source", "inbox_rate", "as_date"))
  change_df <- arrange(change_df, ip_address, isp_source, as_date)
  require(data.table)
  df <- data.table(change_df)
  df$group <- paste(df$ip_address,df$isp_source,sep='_')
  setkey(df,group)
  ee <- df[,diff:=c(NA,diff(inbox_rate)),by=group]    
  change_df2 <- data.frame(ee)
  return(change_df2)
}
#