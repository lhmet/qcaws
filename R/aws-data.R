# aws_read

zfile <- here::here("inst/extdata/2008.zip")

tempd <- fs::path(tempd, readr::parse_number(basename(zfile)))
unzip(zfile, exdir = tempd)
csv_files <- fs::dir_ls(tempd, type = "file")


lookup_varnames <- function() {
  lookup <- c(
    "precipitacao_total_horario_mm",
    "pressao_atmosferica_ao_nivel_da_estacao_horaria_m_b",
    "pressao_atmosferica_max_na_hora_ant_aut_m_b",
    "pressao_atmosferica_min_na_hora_ant_aut_m_b",
    "radiacao_global_kj_m2",
    "temperatura_do_ar_bulbo_seco_horaria_c",
    "temperatura_do_ponto_de_orvalho_c",
    "temperatura_maxima_na_hora_ant_aut_c",
    "temperatura_minima_na_hora_ant_aut_c",
    "temperatura_orvalho_max_na_hora_ant_aut_c",
    "temperatura_orvalho_min_na_hora_ant_aut_c",
    "umidade_rel_max_na_hora_ant_aut_percent",
    "umidade_rel_min_na_hora_ant_aut_percent",
    "umidade_relativa_do_ar_horaria_percent",
    "vento_direcao_horaria_gr_gr",
    "vento_rajada_maxima_m_s",
    "vento_velocidade_horaria_m_s"
  )

  std_varnames <- c(
    "prec",
    "p",
    "pmax",
    "pmin",
    "rg", #"rg_KJ_m2",
    "tair",
    "td",
    "tmax_hprev",
    "tmin_hprev",
    "tdmax_hprev",
    "tdmin_hprev",
    "rhmax_hprev",
    "rhmin_hprev",
    "rh",
    "wd",
    "wsmax",
    "ws"
  )

  names(lookup) <- std_varnames
  lookup
}



## IMPORT ----



aws_data <- function(.aws_file) {

  # .aws_file = csv_files[7]

  checkmate::assert_file_exists(.aws_file)

  code <- data.table::fread(.aws_file, nrows = 3, encoding = "Latin-1") |>
    tidytable::pull(2)
  code <- grep("A[0-9]{3,}", code, value = TRUE)

  data.table::fread(.aws_file,
    skip = 8,
    header = TRUE,
    sep = ";",
    dec = ",",
    encoding = "Latin-1",
    na.strings = "-9999"
  ) |>
    tidytable::mutate(code = code) |>
    tidytable::relocate(code)
}



## CLEAN ----
## check
# funique::funique(nchar(aws_data$hora_utc))




aws_clean <- function(.aws_data, conv_kjm2_wm2 = TRUE) {

  # .aws_data = csv_files[7] |> aws_data()

  lookup_vnames <- lookup_varnames()

  .aws_data <- .aws_data |> #names()
    janitor::clean_names() |> #names()
    tibble::as_tibble() |>
    tidytable::unite(date, data_yyyy_mm_dd, hora_utc, sep = " ") |>
    tidytable::mutate(date = lubridate::ymd_hm(date, tz = "UTC")) |>
    # to remove v20
    janitor::remove_constant()

  checkmate::assert_names(
    names(.aws_data), subset.of = c("date", unname(lookup_vnames))
  )

  .aws_data <- .aws_data |>
    #dplyr::rename(dplyr::all_of(lookup_vnames))
    ## PQ PODE FALTAR UMA VARIAVEL EM ALGUMA ESTACAO COMO 'vento_direcao_horaria_gr_gr'
    dplyr::rename(dplyr::any_of(lookup_vnames))


  # SOME STATIONS DOES'NT HAVE rg
  is_rg_data <- "rg" %in% names(.aws_data)

  if (conv_kjm2_wm2 && is_rg_data)  {
    value_conv_kjm2_wm2 <- (1000/10^6)/0.0864*24
    .aws_data <- .aws_data |>
      tidytable::mutate(rg = rg * value_conv_kjm2_wm2)
  }

  .aws_data

}


## DATA PROC ----



aws_proc <- function(.aws_file){

  message("Processing ", basename(.aws_file), "\n")

  .aws_file |>
    aws_data() |>
    aws_clean(conv_kjm2_wm2 = TRUE)

}


yr_from_files <- function(files){
  files |> basename() |> stringr::str_extract("[0-9]{4}") |> funique::funique()
}

aws_proc_files_yr <- function(files){

  purrr::map(files, aws_proc) |>
    data.table::rbindlist(fill = TRUE)

}

year_files <- yr_from_files(csv_files)

aws_data_inmet <- aws_proc_files_yr(files = csv_files)

qs::qsave(aws_data_inmet, file = here::here("data", paste0(year_files, ".qs")))

#openair::timePlot(aws_data_clean, names(aws_data_clean)[-1])

