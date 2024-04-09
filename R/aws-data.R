# aws_read

down_zfile_year <- function(yr) {
  # yr = 2009
  url <- glue::glue("https://portal.inmet.gov.br/uploads/dadoshistoricos/{yr}.zip")
  dest_file <- fs::path(tempdir(), basename(url))

  Sys.sleep(0.8)
  download.file(url, destfile = dest_file, mode = "wb")

  tictoc::tic()
  csv_files <- unzip(dest_file, exdir = dirname(dest_file))
  tictoc::toc()

  csv_files
}

# csv_files <- down_zfile_year(2010)


# RELACAO ENTRE NOMES NOS DADOS BRUTOS E NOMES PROCESSADOS ----
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
    "rg", # "rg_KJ_m2",
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

type_conv <- purrr::quietly(readr::type_convert)

parse_header <- function(.aws_file){

  cab <- data.table::fread(
    .aws_file,
    nrows = 7,
    encoding = "Latin-1",
    header = FALSE,
    dec = ","
  ) |>
    t() |>
    as.data.frame()
  names(cab) <- cab[1, ]
  cab <- cab[-1, ] |>
    janitor::clean_names() |>
    tibble::as_tibble() |>
    type_conv(
      locale = readr::locale(
        "pt",
        decimal_mark = ","
      )
    ) |>
    purrr::pluck("result") |>
    tidytable::relocate(codigo_wmo) |>
    tidytable::rename("code" = "codigo_wmo",
                      "region" = "regiao",
                      "name" = "estacao",
                      "state" = "uf"
                      )
  cab

}

## IMPORT ----

aws_data <- function(.aws_file, na = "-9999") {
  # .aws_file = csv_files[7]; na = "-9999"
  ## caso 2008
  # .aws_file = grep("A903", csv_files, value = TRUE); na = "-9999"

  checkmate::assert_file_exists(.aws_file)

  # HEADER
  metadata <- parse_header(.aws_file)

  # DATA
  .aws_raw <- data.table::fread(.aws_file,
    skip = 8,
    header = TRUE,
    sep = ";",
    dec = ",",
    encoding = "Latin-1",
    na.strings = na
  ) |>
    tidytable::mutate(code = code) |>
    tidytable::relocate(code) |>
    tibble::as_tibble()

  list(data = .aws_raw, file = .aws_file)
}



## CLEAN ----

.write_log <- function(.aws_data, .aws_file, .keep_raw = TRUE) {
  # logfile
  # basename(.aws_file)
  yr <- .aws_data$date |>
    lubridate::year() |>
    funique::funique()

  aws_code <- .aws_data$code |>
    funique::funique()

  log_info <- tibble::tibble(
    year = yr,
    code = aws_code,
    rg_data = FALSE,
    path = .aws_file
  )

  raw_data_dir <- here::here("inst/extdata/raw")
  if (!fs::dir_exists(raw_data_dir)) fs::dir_create(raw_data_dir)

  .aws_file_raw <- here::here(raw_data_dir, basename(.aws_file))
  if (.keep_raw) fs::file_copy(.aws_file, .aws_file_raw)

  log_file_yr <- here::here("data", "log", glue::glue("{yr}_log.csv"))
  if (!fs::dir_exists(dirname(log_file_yr))) {
    fs::dir_create(dirname(log_file_yr))
  }

  cat("  `rg` variable (column) is missing. ")

  warning(
    "Missing `rg` variable (column) in: \n", .aws_file, "\n",
    glue::glue(
      "Please check the log file \n {log_file_yr} and the original file in \n {.aws_file_raw}. \n"
    )
  )

  # if (fs::file_exists(log_file_yr)) {
  data.table::fwrite(log_info, append = TRUE, file = log_file_yr)
  # } #else {
  # data.table::fwrite(log_info, file = log_file_yr)
  # }

  log_file_yr
}

aws_clean <- function(.aws_raw, conv_kjm2_wm2 = TRUE) {
  # .aws_raw = csv_files[7] |> aws_data(); conv_kjm2_wm2 = TRUE


  lookup_vnames <- lookup_varnames()

  .aws_data <- .aws_raw$data
  # funique::funique(nchar(aws_data$hora_utc))
  .aws_file <- .aws_raw$file

  .aws_data <- .aws_data |> # names()
    janitor::clean_names() |> # names()
    tibble::as_tibble() |>
    tidytable::unite(date, data_yyyy_mm_dd, hora_utc, sep = " ") |>
    tidytable::mutate(date = lubridate::ymd_hm(date, tz = "UTC")) |>
    # to remove v20
    janitor::remove_constant()

  checkmate::assert_names(
    names(.aws_data),
    subset.of = c("date", unname(lookup_vnames))
  )

  .aws_data <- .aws_data |>
    # dplyr::rename(dplyr::all_of(lookup_vnames))
    ## PQ PODE FALTAR UMA VARIAVEL EM ALGUMA ESTACAO COMO 'vento_direcao_horaria_gr_gr'
    dplyr::rename(dplyr::any_of(lookup_vnames))



  # SOME STATIONS DOES'NT HAVE rg
  is_rg_data <- "rg" %in% names(.aws_data)

  if (conv_kjm2_wm2 && is_rg_data) {
    value_conv_kjm2_wm2 <- (1000 / 10^6) / 0.0864 * 24

    .aws_data <- .aws_data |>
      tidytable::mutate(rg = rg * value_conv_kjm2_wm2)

    return(.aws_data)
  }

  if (!is_rg_data) {
    .write_log(.aws_data, .aws_file)
  }

  .aws_data
}


## DATA PROC ----
## IMPORTA E LIMPA UM ARQUIVO DE DADOS BRUTOS

aws_proc <- function(.aws_file) {
  aws_code <- stringr::str_extract(.aws_file, "A[0-9]{3,}")

  message("Processing ", aws_code, "\n")

  .aws_file |>
    aws_data() |>
    aws_clean(conv_kjm2_wm2 = TRUE)
}


## EXTRACT YEAR FROM FILE NAME

yr_from_files <- function(files) {
  files |>
    basename() |>
    stringr::str_extract("[0-9]{4}") |>
    funique::funique()
}

## DATA PROCESSING FOR ONE YEAR (ZIPFILE)


aws_data_proc_year <- function(.csv_files) {
  year_files <- yr_from_files(.csv_files)

  aws_data_inmet <- purrr::map(.csv_files, aws_proc) |>
    data.table::rbindlist(fill = TRUE)

  qs::qsave(aws_data_inmet, file = here::here("data", paste0(year_files, ".qs")))
}


# openair::timePlot(aws_data_clean, names(aws_data_clean)[-1])
csv_files <- down_zfile_year(2008)
aws_inmet_y <- csv_files |>
  aws_data_proc_year()

## DADOS QUE ACUSAM NAO TER RAD GLOBAL, NA VERDADE TEM,
## VERIFICAR SE NOME DA RAD GLOBAL NAO MUDA!
