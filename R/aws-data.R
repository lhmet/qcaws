pacman::p_load(tidyverse)



down_zfile_year <- function(yr, keep = FALSE) {
  # yr = 2000
  url <- glue::glue("https://portal.inmet.gov.br/uploads/dadoshistoricos/{yr}.zip")
  dest_file <- fs::path(tempdir(), basename(url))

  Sys.sleep(0.8)
  download.file(url, destfile = dest_file, mode = "wb")



  if (keep) {
    dest_dir_copy_zip <- here::here("data")
    if (!fs::dir_exists(dest_dir_copy_zip)) fs::dir_create(dest_dir_copy_zip)

    fs::file_copy(dest_file, dest_dir_copy_zip, overwrite = TRUE)
    message(basename(dest_file), " saved in ", dest_dir_copy_zip)
  }

  tictoc::tic()
  csv_files <- unzip(dest_file, exdir = dirname(dest_file))
  tictoc::toc()

  csv_files
}

# csv_files <- down_zfile_year(2000)


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


.count_underscores <- function(.nms = names(.cab)){
  nms <- stringr::str_extract(.nms, "_")
  n_undescores <- sum(nms == "_", na.rm = TRUE)
  n_undescores
}

.fix_header_nms <- function(.cab) {
  # .cab = cab
  # unico "_" esperado eh para o code_wmo, se haver mais de um, provavel problema de encoding

  n_undescores <- .count_underscores(names(.cab))
  if (n_undescores > 2) {
    .cab <- .cab |>
      tidytable::rename(
        "regiao" = "regi_o",
        "estacao" = "estac_o",
        "data_de_fundacao" = "data_de_fundac_o"
      )
  }
  .cab
}

.grep_qtly <- purrr::quietly(grep)

.find_end_header <- function(.awsfile) {

  # .awsfile = .aws_file
  enc <- readr::guess_encoding(.awsfile)[["encoding"]][1]

  .awsfile |>
    readr::read_lines(n_max = 15, locale = readr::locale(encoding = enc)) |>
    #readLines(n = 15, encoding = "Latin-1")  |>
    stringi::stri_trans_general("latin-ascii") %>%
    .grep_qtly(pattern = "^DATA DE FUNDA*.", x = .) |>
    purrr::pluck("result")
}

parse_header <- function(.aws_file) {

  # https://www.pierrerebours.com/2016/12/file-encoding-in-r

  # .aws_file <- csv_files[1]
  # readr::read_csv2(.aws_file, locale = readr::locale("pt", decimal_mark = ","), n_max = 7)

  #.aws_file = here::here("data/log/2000/INMET_NE_BA_A401_SALVADOR_13-05-2000_A_31-12-2000.CSV")
  # .aws_file = here::here("data/log/2023/INMET_CO_GO_A011_SAO SIMAO_01-01-2023_A_31-12-2023.CSV")


  header_end <- .find_end_header(.aws_file)

  cab <- data.table::fread(
    .aws_file,
    nrows = header_end,
    encoding = "Latin-1",
    header = FALSE,
    dec = ","
  ) |>
    t() |>
    as.data.frame()

  names(cab) <- cab[1, ]

  cab <- cab[-1, ] |>
    janitor::clean_names() |>
    tibble::as_tibble()

  # PARA LIDAR CO MUDANCA NO FORMATO DAS DATAS DE FUNCACAO (VER ARQ DE 2000 E 2023)
  if(any(stringr::str_detect(names(cab), "_yyyy_mm_dd"))){
    cab <- setNames(cab, stringr::str_replace(names(cab), "_yyyy_mm_dd", ""))
  }


  cab <-  cab |>
    type_conv(
      locale = readr::locale(
        "pt",
        decimal_mark = ","
      )
    ) |>
    purrr::pluck("result") |>
    tidytable::relocate(codigo_wmo)

  # PARA RESOLVER PROBLEMA DE MUDANCA DE PADRAO DAS DATAS (VER ARQ DE 2000 E 2023)
  if(stringr::str_detect(cab$data_de_fundacao, "\\/")){
   cab <- tidytable::mutate(cab, data_de_fundacao = lubridate::dmy(data_de_fundacao))
  }

  # PARA RESOLVER PROBLEMA DE ENCODING QUE SURGE A PARTIR DE 2019
  if(.count_underscores(names(cab)) > 2)  cab <- .fix_header_nms(cab)


  cab |>
    tidytable::rename(
      "code" = "codigo_wmo",
      "region" = "regiao",
      "name" = "estacao",
      "state" = "uf"
    )
}

## IMPORT ----

aws_data <- function(.aws_file, na = "-9999") {
  # .aws_file = csv_files[1]; na = "-9999"
  ## caso 2008
  # .aws_file = grep("A903", csv_files, value = TRUE); na = "-9999"

  checkmate::assert_file_exists(.aws_file)

  # HEADER
  .metadata <- parse_header(.aws_file)
  aws_code <- .metadata$code |>
    stringr::str_extract("[A-Za-z]{1}[0-9]{3,4}") |>
    stringr::str_to_upper()


  # readLines(.aws_file, skip = 8, n = 20)

  # DATA
  .aws_raw <- data.table::fread(.aws_file,
    skip = .find_end_header(.aws_file),
    header = TRUE,
    sep = ";",
    dec = ",",
    encoding = "Latin-1",
    na.strings = na
  ) |>
    tidytable::mutate(code = aws_code) |>
    tidytable::relocate(code) |>
    tibble::as_tibble()


  list(data = .aws_raw, file = .aws_file, metadata = .metadata)
}




## LOG ISSUES ----

.write_log <- function(.aws_data, .aws_file, vars = vars_empty, type = "MISSING", .keep_raw = TRUE) {
  # logfile
  # basename(.aws_file)

  yr <- .aws_data$date |>
    lubridate::year() |>
    funique::funique()

  aws_code <- .aws_data$code |>
    funique::funique()

  raw_data_dir <- here::here("data", "log", yr)
  if (!fs::dir_exists(raw_data_dir)) fs::dir_create(raw_data_dir)

  .aws_file_raw <- here::here(raw_data_dir, basename(.aws_file))
  if (.keep_raw) fs::file_copy(.aws_file, .aws_file_raw, overwrite = TRUE)

  log_file_yr <- here::here(dirname(raw_data_dir), glue::glue("{yr}_log.csv"))
  # if (!fs::dir_exists(dirname(log_file_yr))) {
  #  fs::dir_create(dirname(log_file_yr))
  # }

  log_info <- tibble::tibble(
    year = yr,
    code = aws_code,
    type = type,
    variable = vars,
    path = fs::path_rel(.aws_file_raw)
  )


  warning(
    glue::glue("{type} all observations of some variable(s) (column(s)) in: \n"), fs::path_rel(.aws_file_raw), "\n",
    glue::glue(
      "Please check the log file \n {fs::path_rel(log_file_yr)}. \n"
    ),
    call. = FALSE
  )

  if (fs::file_exists(log_file_yr)) {
    data.table::fwrite(log_info, append = TRUE, file = log_file_yr)
  } else {
    data.table::fwrite(log_info, file = log_file_yr)
  }

  log_file_yr
}




.check_empty_variables <- function(dat) {
  # dat <- .aws_data; na.rm = FALSE; quiet = TRUE

  # dat <- tidytable::select(dat, -code)

  mask <- sapply(
    X = seq_len(ncol(dat)),
    FUN = function(idx) {
      all(is.na(dat[[idx]]))
    }
  )

  vnames <- names(dat)[mask]

  if (length(vnames) > 0) {
    v <- paste0(vnames, collapse = "\n ")
    message("Variables missing all observations:\n", v, "\n")
  }
  vnames
}


.check_cte_variables <- function(dat) {
  # dat <- .aws_data

  dat <- tidytable::select(dat, -code)

  mask <- sapply(
    X = seq_len(ncol(dat)),
    FUN = function(idx) {
      # idx = 6
      x <- dat[[idx]]
      if (all(is.na(x))) {
        return(FALSE)
      }
      data.table::uniqueN(x[!is.na(x)]) <= 1
    }
  )

  vnames <- names(dat)[mask]

  if (length(vnames) > 0) {
    v <- paste0(vnames, collapse = "\n ")
    message("Variables constant for all observations:\n", v, "\n")
  }

  vnames
}




## CLEAN ----

aws_clean <- function(.aws_raw, conv_kjm2_wm2 = TRUE) {
  # .aws_raw = csv_files[1] |> aws_data(); conv_kjm2_wm2 = TRUE


  lookup_vnames <- lookup_varnames()

  .aws_data <- .aws_raw$data
  # funique::funique(nchar(aws_data$hora_utc))
  .aws_file <- .aws_raw$file
  .aws_meta <- .aws_raw$metadata

  .aws_data <- .aws_data |> # names()
    janitor::clean_names() |>
    tibble::as_tibble()

  # print(names(.aws_data))

  # FIX VARIABLE NAME USED FOR DATE
  nms_to_fix <- names(.aws_data)
  names(.aws_data)[grep("data", nms_to_fix)] <- "data_yyyy_mm_dd"


  # CONVERT DATE HMS
  .aws_data <- .aws_data |>
    tidytable::unite(date, data_yyyy_mm_dd, hora_utc, sep = " ") |>
    tidytable::mutate(date = lubridate::ymd_hm(date, tz = "UTC"))

  # CHECK VARIABLE NAMES
  checkmate::assert_names(
    names(.aws_data),
    must.include = c("code", "date", unname(lookup_vnames))
  )

  # SELECT THE EXPECTED VARIABLES
  .aws_data <- .aws_data |>
    tidytable::select(
      tidytable::all_of(c("code", "date", unname(lookup_vnames)))
    )
  # cat(grep("radiacao_global", names(.aws_data), value = TRUE), "\n")

  .aws_data <- .aws_data |>
    # dplyr::rename(dplyr::all_of(lookup_vnames))
    ## PQ PODE FALTAR UMA VARIAVEL EM ALGUMA ESTACAO COMO 'vento_direcao_horaria_gr_gr'
    dplyr::rename(dplyr::any_of(lookup_vnames))

  # CHECK VARIABLES WITH NO VARIATION
  vars_empty <- .check_empty_variables(.aws_data)
  vars_ctes <- .check_cte_variables(.aws_data)


  if (length(vars_empty) > 0) {
    .write_log(.aws_data, .aws_file, vars = vars_empty, type = "MISSING")
  }
  if (length(vars_ctes) > 0) {
    .write_log(.aws_data, .aws_file, vars = vars_ctes, type = "CONSTANT")
  }

  # SOME STATIONS DOES'NT HAVE rg?
  is_rg_data <- "rg" %in% names(.aws_data)

  if (conv_kjm2_wm2 && is_rg_data) {
    value_conv_kjm2_wm2 <- (1000 / 10^6) / 0.0864 * 24

    .aws_data <- .aws_data |>
      tidytable::mutate(rg = rg * value_conv_kjm2_wm2)

    #return(.aws_data)
  }

  aws_data_clean <- tibble::tibble(
    aws_data = list(.aws_data),
    aws_meta = list(.aws_meta)
    )

  aws_data_clean

}


## DATA PROC ----
## IMPORTA E LIMPA UM ARQUIVO DE DADOS BRUTOS

aws_proc <- function(.aws_file) {
  # .aws_file = csv_files[1]
  aws_code <- stringr::str_extract(.aws_file, "[A-Za-z]{1}[0-9]{3,4}")


  message("Processing ", aws_code)

  .aws_file |>
    aws_data() |> # purrr::pluck("data") |>  janitor::clean_names() |> names()
    aws_clean(conv_kjm2_wm2 = TRUE)
}


## EXTRACT YEAR FROM FILE NAME

yr_from_files <- function(files) {
  # files = csv_files
  files |>
    basename() |>
    stringr::str_extract("-[0-9]{4}_") |>
    funique::funique() |>
    stringr::str_extract("[0-9]{4}")
}

## DATA PROCESSING FOR ONE YEAR (ZIPFILE)


aws_data_proc_year <- function(.csv_files) {

  # .csv_files = csv_files[1:2]

  year_files <- yr_from_files(.csv_files)

  aws_data_inmet <- purrr::map(
    seq_along(.csv_files),
    function(ifile) {
      message("\n", ifile)
      aws_proc(.csv_files[ifile])
    }
  ) |>
    #data.table::rbindlist(fill = TRUE)
    purrr::list_rbind()

  data_year <- purrr::list_rbind(aws_data_inmet[["aws_data"]])
  meta_year <- purrr::list_rbind(aws_data_inmet[["aws_meta"]])

  out_dataf <- here::here("data", paste0(year_files, "-data.qs"))
  out_metaf <- here::here("data", paste0(year_files, "-meta.qs"))
  qs::qsave(data_year, file = out_dataf)
  qs::qsave(meta_year, file = out_metaf)

  tibble::tibble(data = out_dataf, meta = out_metaf)

}


# openair::timePlot(aws_data_clean, names(aws_data_clean)[-1])





data_proc_year <- function(.iyear) {
  # .iyear = 2019
  message(.iyear, " -------------", "\n")
  csv_files <- down_zfile_year(.iyear, keep = FALSE)
  aws_inmet_y <- csv_files |>
    aws_data_proc_year()

  aws_inmet_y
}


data_proc_years <- function(years, par = FALSE) {

  if (par) {

    future::plan(multicore, workers = parallel::detectCores() - 1)
    .data_files_aws_proc <- furrr::future_map(years, data_proc_year, .progress = TRUE)

  } else {

    .data_files_aws_proc <- purrr::map(years, data_proc_year, .progress = TRUE)

  }

  .data_files_aws_proc |>
    purrr::list_rbind()

}


# TRY -----

pacman::p_load(future, furrr)

if (TRUE) {
  yrs <- c(2000, 2023)

  tictoc::tic()
   files_aws <- data_proc_years(yrs, par = FALSE)
  tictoc::toc()

  files_aws

  #VERIFICAR PROBLEMA DA DATA DE FUNDACAO

}


join_aws_files <- function(.files_aws, type = c("meta", "data")) {

  # .files_aws = files_aws; type = "meta"

  .files_aws |>
    magrittr::extract2(type) |>
    purrr::map(\(ifile) qs::qread(ifile)) |>
    purrr::list_rbind() |> tail()
    tidytable::select(code:altitude) |>
    tidytable::distinct()

}


