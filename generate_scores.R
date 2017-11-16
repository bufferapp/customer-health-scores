# load packages
library(buffer)
library(dplyr)
library(tidyr)
library(lubridate)
library(broom)
library(purrr)
library(digest)

# define function to get the update data
get_updates <- function() {

  # connect to redshift
  con <- redshift_connect()

  # define query
  query <- "
  select
    u.user_id
    , u.billing_plan
    , u.billing_stripe_id as customer_id
    , date_trunc('week', u.created_at) as user_created_at
    , date_trunc('week', up.created_at) as update_week
    , count(distinct up.id) as update_count
  from dbt.updates as up
  left join dbt.profiles as p
  on p.id = up.profile_id
  left join users as u
  on p.user_id = u.user_id
  where u.billing_plan != 'individual'
  and u.billing_plan != 'awesome'
  and u.billing_plan != 'new_awesome'
  and u.billing_plan != '1'
  and u.billing_plan is not null
  and up.created_at >= (current_date - 180)
  and up.status <> 'service'
  group by 1, 2, 3, 4, 5
  "

  # run query
  users <- query_db(query, con)

  # return users
  return(users)
}


tidy_data <- function(df) {

  # set dates as date objects
  df$user_created_at <- as.Date(df$user_created_at, format = '%Y-%m-%d')
  df$update_week <- as.Date(df$update_week, format = '%Y-%m-%d')

  # fill out missing values
  df_complete <- df %>%
    filter(update_week != max(df$update_week)) %>%
    complete(user_id, update_week, fill = list(update_count = 0)) %>%
    select(user_id, update_week, update_count) %>%
    left_join(select(df, c(user_id, user_created_at)), by = 'user_id') %>%
    filter(update_week >= user_created_at)

  # get year value
  df_complete <- df_complete %>%
    mutate(year = year(update_week) + yday(update_week) / 365)

  # get the overall update totals and number of weeks for each user
  df_totals <- df_complete %>%
    group_by(user_id) %>%
    summarize(update_total = sum(update_count),
              number_of_weeks = n_distinct(update_week[update_count > 0])) %>%
    filter(number_of_weeks >= 3)

  # only include users that have sent updates in 3 distinct weeks
  update_week_counts <- df_complete %>%
    inner_join(df_totals, by = 'user_id')

  # remove duplicates
  update_week_counts <- unique(update_week_counts)

  # return data
  return(update_week_counts)
}

get_slopes <- function(update_counts) {

  # create logistic regression model
  mod <- ~ glm(cbind(update_count, update_total) ~ year, ., family = "binomial")

  # calculate growth rates for each user (this might take a while)
  slopes <- update_counts %>%
    nest(-user_id) %>%
    mutate(model = map(data, mod)) %>%
    unnest(map(model, tidy)) %>%
    filter(term == "year") %>%
    arrange(desc(estimate))

  # normalize the estimates
  slopes <- slopes %>%
    mutate(at_risk = estimate <= -5 & p.value <= 0.05, created_at_date = Sys.Date()) %>%
    mutate(id = digest(paste0(user_id, created_at_date, estimate), algo = "md5")) %>%
    select(id, user_id, at_risk, estimate, p.value, created_at_date)

  # name columns
  colnames(slopes) <- c('id', 'user_id', 'at_risk', 'score', 'p_value', 'created_at_date')

  # return the estimates
  return(slopes)
}

get_historic_data <- function() {

  # connect to redshift
  con <- redshift_connect()

  # get historic data
  old_df <- query_db("select * from customer_health_scores", con)

  # set column names
  colnames(old_df) <- c('id', 'user_id', 'at_risk', 'score', 'p_value', 'created_at_date')

  # return the old data
  old_df
}

# create an empty Redshift table
create_empty_table <- function(con, tn, df) {

  # Build SQL query
  sql <- paste0("create table \"",tn,"\" (",paste0(collapse=',','"',names(df),'" ',sapply(df[0,],postgresqlDataType)),");");

  # Execute query
  dbSendQuery(con,sql)

  invisible()
}

# fill the empty redshift table
insert_data <- function(con, tn, df, size = 100L) {
  cnt <- (nrow(df)-1L)%/%size+1L

  for (i in seq(0L,len=cnt)) {
    sql <- paste0("insert into \"",tn,"\" values (",do.call(paste,c(sep=',',collapse='),(',lapply(df[seq(i*size+1L,min(nrow(df),(i+1L)*size)),],shQuote))),");");
    dbSendQuery(con,sql);
  }

}

# write the results to a table in Reshift
write_to_redshift <- function(df) {

  # connect to redshift
  con <- redshift_connect()

  # delete existing table
  delete_query <- "drop table customer_health_scores"
  query_db(delete_query, con)

  # create new table
  create_empty_table(con, 'customer_health_scores', df)

  # insert the dataframe
  insert_data(con, 'customer_health_scores', df)
}

main <- function() {

  # get the update counts
  print('Getting updates.')
  updates <- get_updates()

  # tidy the updates
  print('Tidying updates.')
  tidy_updates <- tidy_data(updates)

  # get slope estimates
  print('Fitting GLM models.')
  slopes <- get_slopes(tidy_updates)

  # get historic data
  print('Getting historic data')
  old_scores <- get_historic_data()

  # merge data frames
  all_scores <- rbind(slopes, old_scores)

  # write to redshift
  print('Writing to Redshift.')
  write_to_redshift(all_scores)
  print('Done.')

}

main()
