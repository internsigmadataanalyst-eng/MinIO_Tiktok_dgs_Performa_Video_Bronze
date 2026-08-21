MERGE `database-sigma.Testing.silver_tt_video_production` T
USING (
  -- 1) Ambil snapshot terbaru per (akun, id_konten, tanggal_harian)
  WITH latest_raw AS (
    SELECT * EXCEPT(rn) FROM (
      SELECT b.*,
             ROW_NUMBER() OVER (
               PARTITION BY
                 UPPER(TRIM(b.akun)),
                 UPPER(TRIM(COALESCE(b.id_konten,''))),
                 DATETIME_TRUNC(CAST(b.tanggal AS DATETIME), DAY)
               ORDER BY b.snapshot_ts DESC, b.run_id DESC
             ) rn
      FROM `database-sigma.Testing.bronze_video_production` b
    )
    WHERE rn = 1
  ),

  -- 2) Normalisasi & cast sesuai skema silver
  base AS (
    SELECT
      CAST(tanggal AS DATETIME)                      AS tanggal,
      UPPER(TRIM(scripter))                          AS scripter,
      UPPER(TRIM(editor))                            AS editor,
      UPPER(TRIM(akun))                              AS akun,
      UPPER(TRIM(produk))                            AS produk,
      UPPER(TRIM(jenis_konten))                      AS jenis_konten,
      UPPER(TRIM(tipe_konten))                       AS tipe_konten,
      UPPER(TRIM(talent_visual))                     AS talent_visual,
      UPPER(TRIM(kategori_konten))                   AS kategori_konten,
      UPPER(TRIM(isu))                               AS isu,
      UPPER(TRIM(layout))                            AS layout,
      UPPER(TRIM(script))                            AS script,
      UPPER(TRIM(brief_editing))                     AS brief_editing,
      UPPER(TRIM(cta))                               AS cta,
      UPPER(TRIM(talent_vo))                         AS talent_vo,
      UPPER(TRIM(referensi_musik))                   AS referensi_musik,
      UPPER(TRIM(sound))                             AS sound,
      UPPER(TRIM(visual_hook))                       AS visual_hook,
      UPPER(TRIM(audio_hook))                        AS audio_hook,
      UPPER(TRIM(progress_edit))                     AS progress_edit,
      CAST(tanggal_jadi AS DATETIME)                 AS tanggal_jadi,
      UPPER(TRIM(progres_upload))                    AS progres_upload,
      TRIM(link_konten)                              AS link_konten,
      UPPER(TRIM(id_konten))                         AS id_konten,
      UPPER(TRIM(hook))                              AS hook,
      UPPER(TRIM(tingkat_konten))                    AS tingkat_konten,
      snapshot_ts, snapshot_date, run_id, row_hash_raw
    FROM latest_raw
  ),

  -- 3) Hitung row_hash_clean dari field yang relevan
  with_hash AS (
    SELECT
      b.*,
      TO_HEX(SHA256(ARRAY_TO_STRING([
        FORMAT_DATETIME('%F %T', b.tanggal),
        b.akun, COALESCE(b.id_konten,''),
        COALESCE(b.scripter,''), COALESCE(b.editor,''), COALESCE(b.produk,''),
        COALESCE(b.jenis_konten,''), COALESCE(b.tipe_konten,''), COALESCE(b.talent_visual,''),
        COALESCE(b.kategori_konten,''), COALESCE(b.isu,''), COALESCE(b.layout,''),
        COALESCE(b.script,''), COALESCE(b.brief_editing,''), COALESCE(b.cta,''),
        COALESCE(b.talent_vo,''), COALESCE(b.referensi_musik,''), COALESCE(b.sound,''),
        COALESCE(b.visual_hook,''), COALESCE(b.audio_hook,''), COALESCE(b.progress_edit,''),
        COALESCE(b.hook,''), COALESCE(b.tingkat_konten,''),
        FORMAT_DATETIME('%F %T', b.tanggal_jadi),
        COALESCE(b.progres_upload,''), COALESCE(b.link_konten,'')
      ], '||'))) AS row_hash_clean
    FROM base b
  )

  SELECT * FROM with_hash
) S
ON  T.tanggal = S.tanggal
AND UPPER(TRIM(T.akun)) = S.akun
AND COALESCE(UPPER(TRIM(T.id_konten)),'') = COALESCE(S.id_konten,'')
WHEN MATCHED AND T.row_hash_clean != S.row_hash_clean THEN
  UPDATE SET
    scripter        = S.scripter,
    editor          = S.editor,
    produk          = S.produk,
    jenis_konten    = S.jenis_konten,
    tipe_konten     = S.tipe_konten,
    talent_visual   = S.talent_visual,
    kategori_konten = S.kategori_konten,
    isu             = S.isu,
    layout          = S.layout,
    script          = S.script,
    brief_editing   = S.brief_editing,
    cta             = S.cta,
    talent_vo       = S.talent_vo,
    referensi_musik = S.referensi_musik,
    sound           = S.sound,
    visual_hook     = S.visual_hook,
    audio_hook      = S.audio_hook,
    progress_edit   = S.progress_edit,
    tanggal_jadi    = S.tanggal_jadi,
    progres_upload  = S.progres_upload,
    link_konten     = S.link_konten,
    snapshot_ts     = S.snapshot_ts,
    snapshot_date   = S.snapshot_date,
    run_id          = S.run_id,
    row_hash_raw    = S.row_hash_raw,
    row_hash_clean  = S.row_hash_clean,
    hook            = S.hook,
    tingkat_konten  = S.tingkat_konten
WHEN NOT MATCHED THEN
  INSERT (
    tanggal, scripter, editor, akun, produk, jenis_konten, tipe_konten,
    talent_visual, kategori_konten, isu, layout, script, brief_editing, cta,
    talent_vo, referensi_musik, sound, visual_hook, audio_hook, progress_edit,
    tanggal_jadi, progres_upload, link_konten, id_konten, hook, tingkat_konten,
    snapshot_ts, snapshot_date, run_id, row_hash_raw, row_hash_clean
  )
  VALUES (
    S.tanggal, S.scripter, S.editor, S.akun, S.produk, S.jenis_konten, S.tipe_konten,
    S.talent_visual, S.kategori_konten, S.isu, S.layout, S.script, S.brief_editing, S.cta,
    S.talent_vo, S.referensi_musik, S.sound, S.visual_hook, S.audio_hook, S.progress_edit,
    S.tanggal_jadi, S.progres_upload, S.link_konten, S.id_konten, S.hook, S.tingkat_konten,
    S.snapshot_ts, S.snapshot_date, S.run_id, S.row_hash_raw, S.row_hash_clean
  )