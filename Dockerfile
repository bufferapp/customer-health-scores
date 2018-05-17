FROM rocker/tidyverse:3.5

RUN install2.r --error \
    -r 'http://cran.rstudio.com' \
    digest

RUN installGithub.r \
    jwinternheimer/buffer \
    && rm -rf /tmp/downloaded_packages/ /tmp/*.rds

ADD generate_scores.R generate_scores.R

CMD ["Rscript", "generate_scores.R"]
