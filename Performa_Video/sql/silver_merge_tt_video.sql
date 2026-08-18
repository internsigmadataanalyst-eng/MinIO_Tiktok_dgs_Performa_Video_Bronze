MERGE `database-sigma.SILVER_DB.silver_tt_video` T
USING (
  -- ambil snapshot terbaru per (toko, id_kreator, id_video, tanggal)
  WITH latest_raw AS (
    SELECT * EXCEPT(rn) FROM (
      SELECT b.*,
             ROW_NUMBER() OVER (
               PARTITION BY UPPER(TRIM(b.toko)),
                            UPPER(TRIM(COALESCE(b.id_kreator,''))),
                            UPPER(TRIM(COALESCE(b.id_video,''))),
                            DATE(b.tanggal)
               ORDER BY b.snapshot_ts DESC, b.run_id DESC
             ) rn
      FROM `database-sigma.BRONZE_DB.bronze_video` b
    )
    WHERE rn = 1
  ),

  base AS (
    SELECT
      DATE(tanggal)                   AS tanggal,
      UPPER(TRIM(toko))               AS toko,
      UPPER(TRIM(nama_kreator))       AS nama_kreator,
      UPPER(TRIM(id_kreator))         AS id_kreator,
      UPPER(TRIM(informasi_video))    AS informasi_video,
      UPPER(TRIM(id_video))           AS id_video,
      waktu,                          -- sudah DATETIME di bronze
      UPPER(TRIM(produk))             AS produk,

      -- metrik integer → INT64 / uang → NUMERIC
      SAFE_CAST(vv AS INT64)                                 AS vv,
      SAFE_CAST(likes AS INT64)                              AS likes,
      SAFE_CAST(komentar AS INT64)                           AS komentar,
      SAFE_CAST(dibagikan AS INT64)                          AS dibagikan,
      SAFE_CAST(pengikut_baru AS INT64)                      AS pengikut_baru,
      SAFE_CAST(klik_video_ke_live AS INT64)                 AS klik_video_ke_live,
      SAFE_CAST(produk_dilihat AS INT64)                     AS produk_dilihat,
      SAFE_CAST(klik_produk AS INT64)                        AS klik_produk,
      SAFE_CAST(pembeli AS INT64)                            AS pembeli,
      SAFE_CAST(pesanan_video AS INT64)                      AS pesanan_video,
      SAFE_CAST(produk_yang_terjual_dari_video AS INT64)     AS produk_terjual_video,
      SAFE_CAST(nilai_barang_dagangan_bruto_video_rp AS NUMERIC) AS gmv_bruto_video,
      SAFE_CAST(gpm_rp AS NUMERIC)                           AS gpm,
      SAFE_CAST(gmv_yang_didapat_dari_video_jualan_rp AS NUMERIC) AS gmv_didapat_video,

      -- persentase string "12.3%" -> 0.123
      SAFE_CAST(REGEXP_REPLACE(rasio_klik_tayang_video, r'[%\\s]', '') AS FLOAT64)/100 AS ctr_video,
      SAFE_CAST(REGEXP_REPLACE(rasio_video_ke_live, r'[%\\s]', '') AS FLOAT64)/100     AS ratio_video_ke_live,
      SAFE_CAST(REGEXP_REPLACE(persentase_video_yang_ditonton_hingga_selesai, r'[%\\s]', '') AS FLOAT64)/100 AS pct_tonton_selesai,
      SAFE_CAST(REGEXP_REPLACE(rasio_pesanan_per_klik_video, r'[%\\s]', '') AS FLOAT64)/100 AS cvr_video,

      snapshot_ts, snapshot_date, run_id, row_hash_raw
    FROM latest_raw
  ),

  with_hash AS (
    SELECT
      b.*,
      TO_HEX(SHA256(
        ARRAY_TO_STRING([
          FORMAT_DATE('%F', b.tanggal),
          b.toko, COALESCE(b.id_kreator,''), COALESCE(b.id_video,''),
          COALESCE(b.nama_kreator,''), COALESCE(b.informasi_video,''), COALESCE(b.produk,''),
          CAST(b.waktu AS STRING),
          CAST(b.vv AS STRING), CAST(b.likes AS STRING), CAST(b.komentar AS STRING), CAST(b.dibagikan AS STRING),
          CAST(b.pengikut_baru AS STRING), CAST(b.klik_video_ke_live AS STRING),
          CAST(b.produk_dilihat AS STRING), CAST(b.klik_produk AS STRING), CAST(b.pembeli AS STRING),
          CAST(b.pesanan_video AS STRING), CAST(b.produk_terjual_video AS STRING),
          CAST(b.gmv_bruto_video AS STRING), CAST(b.gpm AS STRING), CAST(b.gmv_didapat_video AS STRING),
          CAST(b.ctr_video AS STRING), CAST(b.ratio_video_ke_live AS STRING),
          CAST(b.pct_tonton_selesai AS STRING), CAST(b.cvr_video AS STRING)
        ], '||')
      )) AS row_hash_clean
    FROM base b
  )
  SELECT * FROM with_hash
) S
ON  T.tanggal    = S.tanggal
AND T.toko       = S.toko
AND COALESCE(T.id_kreator,'') = COALESCE(S.id_kreator,'')
AND COALESCE(T.id_video  ,'') = COALESCE(S.id_video  ,'')
WHEN MATCHED AND T.row_hash_clean != S.row_hash_clean THEN
  UPDATE SET
    nama_kreator = S.nama_kreator,
    informasi_video = S.informasi_video,
    waktu = S.waktu,
    produk = S.produk,
    vv = S.vv, likes = S.likes, komentar = S.komentar, dibagikan = S.dibagikan,
    pengikut_baru = S.pengikut_baru, klik_video_ke_live = S.klik_video_ke_live,
    produk_dilihat = S.produk_dilihat, klik_produk = S.klik_produk,
    pembeli = S.pembeli, pesanan_video = S.pesanan_video, produk_terjual_video = S.produk_terjual_video,
    gmv_bruto_video = S.gmv_bruto_video, gpm = S.gpm, gmv_didapat_video = S.gmv_didapat_video,
    ctr_video = S.ctr_video, ratio_video_ke_live = S.ratio_video_ke_live,
    pct_tonton_selesai = S.pct_tonton_selesai, cvr_video = S.cvr_video,
    snapshot_ts = S.snapshot_ts, snapshot_date = S.snapshot_date, run_id = S.run_id,
    row_hash_raw = S.row_hash_raw, row_hash_clean = S.row_hash_clean
WHEN NOT MATCHED THEN
  INSERT (
    tanggal, toko, nama_kreator, id_kreator, informasi_video, id_video, waktu, produk,
    vv, likes, komentar, dibagikan, pengikut_baru, klik_video_ke_live,
    produk_dilihat, klik_produk, pembeli, pesanan_video, produk_terjual_video,
    gmv_bruto_video, gpm, gmv_didapat_video,
    ctr_video, ratio_video_ke_live, pct_tonton_selesai, cvr_video,
    snapshot_ts, snapshot_date, run_id, row_hash_raw, row_hash_clean
  )
  VALUES (
    S.tanggal, S.toko, S.nama_kreator, S.id_kreator, S.informasi_video, S.id_video, S.waktu, S.produk,
    S.vv, S.likes, S.komentar, S.dibagikan, S.pengikut_baru, S.klik_video_ke_live,
    S.produk_dilihat, S.klik_produk, S.pembeli, S.pesanan_video, S.produk_terjual_video,
    S.gmv_bruto_video, S.gpm, S.gmv_didapat_video,
    S.ctr_video, S.ratio_video_ke_live, S.pct_tonton_selesai, S.cvr_video,
    S.snapshot_ts, S.snapshot_date, S.run_id, S.row_hash_raw, S.row_hash_clean
  );