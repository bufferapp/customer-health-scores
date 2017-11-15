FROM rocker/tidyverse

RUN install2.r --error \
    -r 'http://cran.rstudio.com' \
  && installGithub.r \
    jwinternheimer/buffer \
  && rm -rf /tmp/downloaded_packages/ /tmp/*.rds

ADD generate_scores.R generate_scores.R
CMD ["Rscript", "generate_scores.R"]
