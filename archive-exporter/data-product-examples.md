# Data Product Examples

We're getting closer to a Parquet archive format that we're happy with for preserving data and Francisco has some really interesting work of potentially
building a hybrid query layer on top of InfluxDB and this for longer term historial data.

That being said, it's probably the wrong format once a user narrows in on a specific application. Since it's just an export of our InfluxDB data, it's still a mess if you "query everything" and is basically impossible to use in this form:

```sql
select * from 'archive/**/*.parquet';

┌──────────────────────────────┬──────────────────────┬──────────────────────┬─────────┬───┬─────────────┬───────────┬──────────────────────┬────────────┐
│             time             │         name         │        plugin        │   vsn   │ … │ value_float │ value_int │      value_str       │    date    │
│   timestamp with time zone   │       varchar        │       varchar        │ varchar │ … │   double    │   int64   │       varchar        │    date    │
├──────────────────────────────┼──────────────────────┼──────────────────────┼─────────┼───┼─────────────┼───────────┼──────────────────────┼────────────┤
│ 2026-01-01 00:55:15.338592+… │ status               │ registry.sagecontin… │ W08B    │ … │        NULL │      NULL │ Found 288 recent fi… │ 2026-01-01 │
│ 2026-01-01 06:03:16.348756+… │ status               │ registry.sagecontin… │ W08B    │ … │        NULL │      NULL │ Found 73 recent fil… │ 2026-01-01 │
│ 2026-01-01 06:03:45.527823+… │ upload               │ registry.sagecontin… │ W08B    │ … │        NULL │      NULL │ https://storage.sag… │ 2026-01-01 │
│ 2026-01-01 12:03:16.014792+… │ status               │ registry.sagecontin… │ W08B    │ … │        NULL │      NULL │ Found 145 recent fi… │ 2026-01-01 │
│ 2026-01-01 18:03:25.55669+00 │ status               │ registry.sagecontin… │ W08B    │ … │        NULL │      NULL │ Found 217 recent fi… │ 2026-01-01 │
│ 2026-01-01 00:37:16.055243+… │ error                │ registry.sagecontin… │ W08D    │ … │        NULL │      NULL │ No recent files fou… │ 2026-01-01 │
│ 2026-01-01 06:03:16.228316+… │ error                │ registry.sagecontin… │ W08D    │ … │        NULL │      NULL │ No recent files fou… │ 2026-01-01 │
│              ·               │          ·           │          ·           │  ·      │ … │           · │        ·  │  ·                   │     ·      │
│              ·               │          ·           │          ·           │  ·      │ … │           · │        ·  │  ·                   │     ·      │
│              ·               │          ·           │          ·           │  ·      │ … │           · │        ·  │  ·                   │     ·      │
│ 2026-02-01 23:58:35.498497+… │ env.raingauge.total… │ waggle/plugin-raing… │ W0AA    │ … │      643.97 │      NULL │ NULL                 │ 2026-02-01 │
│ 2026-02-01 23:59:05.538568+… │ env.raingauge.rint   │ waggle/plugin-raing… │ W0AA    │ … │         0.0 │      NULL │ NULL                 │ 2026-02-01 │
│ 2026-02-01 23:59:05.538568+… │ env.raingauge.event… │ waggle/plugin-raing… │ W0AA    │ … │         0.0 │      NULL │ NULL                 │ 2026-02-01 │
│ 2026-02-01 23:59:05.538568+… │ env.raingauge.total… │ waggle/plugin-raing… │ W0AA    │ … │      643.97 │      NULL │ NULL                 │ 2026-02-01 │
│ 2026-02-01 23:59:35.563388+… │ env.raingauge.rint   │ waggle/plugin-raing… │ W0AA    │ … │         0.0 │      NULL │ NULL                 │ 2026-02-01 │
│ 2026-02-01 23:59:35.563388+… │ env.raingauge.event… │ waggle/plugin-raing… │ W0AA    │ … │         0.0 │      NULL │ NULL                 │ 2026-02-01 │
│ 2026-02-01 23:59:35.563388+… │ env.raingauge.total… │ waggle/plugin-raing… │ W0AA    │ … │      643.97 │      NULL │ NULL                 │ 2026-02-01 │
└──────────────────────────────┴──────────────────────┴──────────────────────┴─────────┴───┴─────────────┴───────────┴──────────────────────┴────────────┘
  177.42 million rows (40 shown, 177415919 total)                          use .last to show entire result                          14 columns (8 shown)
```

That being said, this format is very amenable to creating derived data products, since it's all just Parquet files. These dervied data products
are probably where a lot of value lives and are things we can maintain and DOI easily. Even better, using tools like duckdb, some of these can be
created using a single SQL query. Here are a few examples:

## Avian Diversity Data Product

This creates a single table with __all the avian diversity data we've ever collected__ in it.

```sql
select time, vsn, string_split(name, '.')[-1] as scientific_name, value_str::DOUBLE as confidence
from 'archive/**/*.parquet'
where name like 'env.detection.avian.%'
order by vsn, time;

┌───────────────────────────────┬─────────┬─────────────────────────────────┬─────────────┐
│             time              │   vsn   │         scientific_name         │ confidence  │
│   timestamp with time zone    │ varchar │             varchar             │   double    │
├───────────────────────────────┼─────────┼─────────────────────────────────┼─────────────┤
│ 2026-01-01 00:11:52.762931+00 │ W020    │ pterocles_senegallus            │  0.13282172 │
│ 2026-01-01 01:46:41.023839+00 │ W020    │ botaurus_stellaris              │  0.12436032 │
│ 2026-01-01 04:51:04.224326+00 │ W020    │ rupicola_peruvianus             │  0.12625012 │
│ 2026-01-01 05:22:12.735252+00 │ W020    │ rallus_longirostris             │ 0.102135845 │
│ 2026-01-01 08:15:30.81584+00  │ W020    │ lanius_cabanisi                 │  0.10003114 │
│ 2026-01-01 13:53:24.224204+00 │ W020    │ lophonetta_specularioides       │  0.21296099 │
│ 2026-01-01 13:53:24.224204+00 │ W020    │ anas_platyrhynchos              │   0.1367102 │
│ 2026-01-01 13:59:12.694267+00 │ W020    │ culicivora_caudacuta            │  0.16242477 │
│ 2026-01-01 13:59:12.694267+00 │ W020    │ mareca_strepera                 │  0.31527483 │
│ 2026-01-01 14:04:52.64061+00  │ W020    │ butastur_indicus                │   0.2772199 │
│ 2026-01-01 14:04:52.64061+00  │ W020    │ acropternis_orthonyx            │  0.31309128 │
│ 2026-01-01 14:15:51.15631+00  │ W020    │ limosa_fedoa                    │  0.15807587 │
│ 2026-01-01 14:59:51.000825+00 │ W020    │ coracias_abyssinicus            │   0.3297347 │
│ 2026-01-01 16:28:30.872542+00 │ W020    │ botaurus_stellaris              │  0.18398473 │
│ 2026-01-01 17:03:22.729957+00 │ W020    │ batrachostomus_auritus          │  0.16997702 │
│ 2026-01-01 18:08:11.013054+00 │ W020    │ botaurus_stellaris              │  0.11960023 │
│ 2026-01-01 20:11:02.663339+00 │ W020    │ lanius_cabanisi                 │  0.11304524 │
│ 2026-01-01 20:12:52.727125+00 │ W020    │ lanius_cabanisi                 │  0.10022518 │
│ 2026-01-01 21:52:42.627671+00 │ W020    │ theristicus_melanopis           │  0.17669824 │
│ 2026-01-01 22:11:54.156773+00 │ W020    │ rallus_elegans                  │   0.2766544 │
│              ·                │  ·      │       ·                         │       ·     │
│              ·                │  ·      │       ·                         │       ·     │
│              ·                │  ·      │       ·                         │       ·     │
│ 2026-02-01 20:10:51.05944+00  │ W0AA    │ melanerpes_formicivorus         │  0.28968745 │
│ 2026-02-01 20:10:51.05944+00  │ W0AA    │ sitta_carolinensis              │  0.19816318 │
│ 2026-02-01 20:11:22.443601+00 │ W0AA    │ nothura_darwinii                │  0.12052207 │
│ 2026-02-01 20:56:22.340976+00 │ W0AA    │ leptosittaca_branickii          │  0.12656379 │
│ 2026-02-01 21:06:20.985542+00 │ W0AA    │ lanius_cabanisi                 │ 0.116378695 │
│ 2026-02-01 21:13:19.199051+00 │ W0AA    │ corvus_corax                    │   0.2080429 │
│ 2026-02-01 21:13:19.199051+00 │ W0AA    │ otus_magicus                    │  0.10744202 │
│ 2026-02-01 21:23:14.243894+00 │ W0AA    │ phalaenoptilus_nuttallii        │  0.16632432 │
│ 2026-02-01 21:23:46.032104+00 │ W0AA    │ bostrychia_rara                 │  0.13834983 │
│ 2026-02-01 21:23:46.032104+00 │ W0AA    │ nothura_darwinii                │  0.11671719 │
│ 2026-02-01 21:26:21.037091+00 │ W0AA    │ lanius_cabanisi                 │  0.10227089 │
│ 2026-02-01 21:35:51.033586+00 │ W0AA    │ lanius_cabanisi                 │ 0.101504266 │
│ 2026-02-01 22:05:51.21761+00  │ W0AA    │ nothura_darwinii                │   0.1674942 │
│ 2026-02-01 22:50:50.991923+00 │ W0AA    │ bubo_virginianus                │  0.19895163 │
│ 2026-02-01 23:00:52.433814+00 │ W0AA    │ campylorhynchus_brunneicapillus │  0.14294823 │
│ 2026-02-01 23:00:52.433814+00 │ W0AA    │ gallinago_nobilis               │  0.21554175 │
│ 2026-02-01 23:00:52.433814+00 │ W0AA    │ lophonetta_specularioides       │  0.10748795 │
│ 2026-02-01 23:01:24.438192+00 │ W0AA    │ campylorhynchus_brunneicapillus │  0.10952041 │
│ 2026-02-01 23:01:24.438192+00 │ W0AA    │ lophonetta_specularioides       │  0.13016579 │
│ 2026-02-01 23:20:51.081317+00 │ W0AA    │ campylorhynchus_brunneicapillus │  0.14752702 │
└───────────────────────────────┴─────────┴─────────────────────────────────┴─────────────┘
  111736 rows (40 shown)            use .last to show entire result             4 columns
Run Time (s): real 0.084 user 1.484563 sys 0.126594
```

Side note.. the single parquet file is only about ~15MB... I suspect a lot of our data falls into this bucket so we can have quite a few data products which are just a single file that can be downloaded in seconds.

This would make it trivial for users to fetch that data and run analysis code like:

```sql
select scientific_name, count(*) as count from detections
where confidence > 0.5
group by scientific_name
order by count;

┌─────────────────────────────┬───────┐
│       scientific_name       │ count │
│           varchar           │ int64 │
├─────────────────────────────┼───────┤
│ myadestes_melanops          │     1 │
│ glaucidium_passerinum       │     1 │
│ actenoides_lindsayi         │     1 │
│ bycanistes_albotibialis     │     1 │
│ procnias_nudicollis         │     1 │
│ upupa_epops                 │     1 │
│ poecilostreptus_cabanisi    │     1 │
│ gorsachius_melanolophus     │     1 │
│ aulacorhynchus_haematopygus │     1 │
│ tragopan_blythii            │     1 │
│ abroscopus_albogularis      │     1 │
│ lalage_leucomela            │     1 │
│ larus_smithsonianus         │     1 │
│ crypturellus_obsoletus      │     1 │
│ asio_flammeus               │     1 │
│ caprimulgus_clarus          │     1 │
│ creagrus_furcatus           │     1 │
│ clibanornis_rectirostris    │     1 │
│ quiscalus_mexicanus         │     1 │
│ haliaeetus_leucogaster      │     1 │
│       ·                     │     · │
│       ·                     │     · │
│       ·                     │     · │
│ rhea_americana              │    29 │
│ ortalis_vetula              │    30 │
│ rupicola_peruvianus         │    36 │
│ otus_scops                  │    37 │
│ otidiphaps_nobilis          │    38 │
│ cathartes_aura              │    38 │
│ pica_hudsonia               │    40 │
│ cygnus_buccinator           │    44 │
│ megascops_asio              │    46 │
│ megapodius_cumingii         │    49 │
│ napothera_danjoui           │    50 │
│ gavia_stellata              │    52 │
│ geronticus_eremita          │    57 │
│ aegolius_acadicus           │    59 │
│ turdus_migratorius          │    68 │
│ eurystomus_orientalis       │    97 │
│ grallaria_rufocinerea       │   165 │
│ bucorvus_leadbeateri        │   302 │
│ podargus_strigoides         │   518 │
│ tympanuchus_cupido          │  1653 │
└─────────────────────────────┴───────┘
  326 rows (40 shown)       2 columns
Run Time (s): real 0.004 user 0.010851 sys 0.000000
```

## Air Quality Plugin Data Product

Here, we export a wide table from the `registry.sagecontinuum.org/seanshahkarami/air-quality:0.3.0` plugin.

```sql
select
    time,
    vsn,
    any_value(value_float) filter (where name = 'env.temperature')       as t,
    any_value(value_float) filter (where name = 'env.pressure')          as p,
    any_value(value_float) filter (where name = 'env.relative_humidity') as rh,
    any_value(value_float) filter (where name = 'env.air_quality.conc')  as conc,
    any_value(value_float) filter (where name = 'env.air_quality.flow')  as flow
from 'archive/**/*.parquet'
where plugin = 'registry.sagecontinuum.org/seanshahkarami/air-quality:0.3.0' and name in (
    'env.temperature', 'env.pressure', 'env.relative_humidity',
    'env.air_quality.conc', 'env.air_quality.flow'
)
group by time, vsn
order by vsn, time;

┌───────────────────────────────┬─────────┬────────┬────────────────────┬────────┬────────┬────────┐
│             time              │   vsn   │   t    │         p          │   rh   │  conc  │  flow  │
│   timestamp with time zone    │ varchar │ double │       double       │ double │ double │ double │
├───────────────────────────────┼─────────┼────────┼────────────────────┼────────┼────────┼────────┤
│ 2026-01-21 20:53:17.506654+00 │ W01B    │    5.0 │           107600.0 │   0.29 │   NULL │    2.0 │
│ 2026-01-21 20:53:19.850731+00 │ W01B    │    5.0 │           107620.0 │   0.29 │  0.001 │    2.0 │
│ 2026-01-21 20:53:19.885049+00 │ W01B    │    5.0 │ 107609.99999999999 │   0.29 │  0.002 │    2.0 │
│ 2026-01-21 20:53:21.295051+00 │ W01B    │    5.0 │ 107609.99999999999 │   0.29 │  0.002 │    2.0 │
│ 2026-01-21 20:56:35.215038+00 │ W01B    │    4.9 │           107580.0 │   0.29 │   NULL │    2.0 │
│ 2026-01-21 20:57:47.233215+00 │ W01B    │    5.1 │ 107509.99999999999 │   0.29 │  0.002 │    2.0 │
│ 2026-01-21 20:58:08.175539+00 │ W01B    │    5.1 │           107520.0 │   0.28 │  0.001 │    2.0 │
│ 2026-01-21 20:58:08.1832+00   │ W01B    │    5.2 │           107530.0 │   0.29 │  0.001 │    2.0 │
│ 2026-01-21 21:01:56.111171+00 │ W01B    │    5.1 │ 107540.00000000001 │   0.28 │   NULL │    2.0 │
│ 2026-01-21 21:01:57.128048+00 │ W01B    │    5.1 │           107530.0 │   0.28 │  0.002 │    2.0 │
│ 2026-01-21 21:01:58.110559+00 │ W01B    │    5.1 │ 107540.00000000001 │   0.28 │  0.002 │    2.0 │
│ 2026-01-21 21:05:23.015649+00 │ W01B    │    6.2 │           107430.0 │   0.28 │   NULL │    2.0 │
│ 2026-01-21 21:05:23.205033+00 │ W01B    │    6.1 │           107470.0 │   0.28 │  0.001 │    2.0 │
│ 2026-01-21 21:05:23.406423+00 │ W01B    │    6.0 │           107470.0 │   0.28 │  0.001 │    2.0 │
│ 2026-01-21 21:05:27.060729+00 │ W01B    │    6.0 │ 107490.00000000001 │   0.28 │  0.002 │    2.0 │
│ 2026-01-21 21:05:31.604384+00 │ W01B    │    6.0 │           107470.0 │   0.28 │  0.002 │    2.0 │
│ 2026-01-21 21:05:31.666658+00 │ W01B    │    5.9 │ 107490.00000000001 │   0.28 │  0.002 │    2.0 │
│ 2026-01-21 21:05:31.752099+00 │ W01B    │    5.9 │ 107440.00000000001 │   0.28 │  0.002 │    2.0 │
│ 2026-01-21 21:05:32.073057+00 │ W01B    │    5.9 │ 107440.00000000001 │   0.28 │  0.002 │    2.0 │
│ 2026-01-21 21:06:43.663119+00 │ W01B    │    5.4 │           107470.0 │   0.28 │  0.002 │    2.0 │
│               ·               │  ·      │     ·  │               ·    │     ·  │    ·   │     ·  │
│               ·               │  ·      │     ·  │               ·    │     ·  │    ·   │     ·  │
│               ·               │  ·      │     ·  │               ·    │     ·  │    ·   │     ·  │
│ 2026-02-01 23:59:37.234931+00 │ W06F    │    2.6 │            79870.0 │   0.28 │  0.002 │    2.0 │
│ 2026-02-01 23:59:38.46085+00  │ W06F    │    2.6 │            79860.0 │   0.28 │  0.002 │    2.0 │
│ 2026-02-01 23:59:40.234709+00 │ W06F    │    2.6 │            79870.0 │   0.28 │  0.002 │    2.0 │
│ 2026-02-01 23:59:41.234724+00 │ W06F    │    2.6 │            79840.0 │   0.28 │  0.002 │    2.0 │
│ 2026-02-01 23:59:42.234636+00 │ W06F    │    2.6 │            79860.0 │   0.28 │  0.002 │    2.0 │
│ 2026-02-01 23:59:43.234547+00 │ W06F    │    2.6 │            79840.0 │   0.28 │  0.001 │    2.0 │
│ 2026-02-01 23:59:44.234464+00 │ W06F    │    2.7 │            79850.0 │   0.28 │  0.002 │    2.0 │
│ 2026-02-01 23:59:45.234503+00 │ W06F    │    2.7 │            79850.0 │   0.28 │  0.001 │    2.0 │
│ 2026-02-01 23:59:46.418382+00 │ W06F    │    2.7 │            79860.0 │   0.28 │  0.001 │    2.0 │
│ 2026-02-01 23:59:48.23838+00  │ W06F    │    2.7 │            79860.0 │   0.28 │  0.002 │    2.0 │
│ 2026-02-01 23:59:49.234169+00 │ W06F    │    2.7 │            79860.0 │   0.28 │  0.002 │    2.0 │
│ 2026-02-01 23:59:50.234104+00 │ W06F    │    2.7 │            79850.0 │   0.28 │  0.002 │    2.0 │
│ 2026-02-01 23:59:51.234062+00 │ W06F    │    2.7 │            79830.0 │   0.28 │  0.003 │    2.0 │
│ 2026-02-01 23:59:52.233997+00 │ W06F    │    2.7 │            79870.0 │   0.28 │  0.002 │    2.0 │
│ 2026-02-01 23:59:53.234058+00 │ W06F    │    2.8 │            79840.0 │   0.28 │  0.003 │    2.0 │
│ 2026-02-01 23:59:54.371218+00 │ W06F    │    2.8 │            79850.0 │   0.28 │  0.002 │    2.0 │
│ 2026-02-01 23:59:56.235828+00 │ W06F    │    2.8 │            79860.0 │   0.28 │  0.002 │    2.0 │
│ 2026-02-01 23:59:57.24272+00  │ W06F    │    2.8 │            79840.0 │   0.28 │  0.002 │    2.0 │
│ 2026-02-01 23:59:58.242712+00 │ W06F    │    2.8 │            79840.0 │   0.28 │  0.002 │    2.0 │
│ 2026-02-01 23:59:59.242694+00 │ W06F    │    2.8 │            79850.0 │   0.28 │  0.002 │    2.0 │
└───────────────────────────────┴─────────┴────────┴────────────────────┴────────┴────────┴────────┘
  5.14 million rows (40 shown, 5138254 total)      use .last to show entire result       7 columns
Run Time (s): real 0.460 user 11.042163 sys 0.674537
```

One nice thing is, since all the relevant columns exist in a wide table, computing things like dewpoint is easy. For example:

```sql
create or replace macro dewpoint(t, rh) as (
  243.04 * (ln(nullif(rh, 0) / 100.0) + 17.625 * t / (243.04 + t))
        / (17.625 - ln(nullif(rh, 0) / 100.0) - 17.625 * t / (243.04 + t))
);

select *, dewpoint(t, rh) as dp, t - dp as dp_gap from 'aqt_data.parquet';
```

## Other Notes

Obviously with enough data, these will become slower. But, our archive structure chunks by date:

```
archive/
    date=2026-01-01
        data_0.parquet
    date=2026-01-02
        data_0.parquet
    date=2026-01-03
        data_0.parquet
    date=2026-01-04
        data_0.parquet
...
```

So, for each data product, we can take various approaches like rollup and cache day wise / week wise / month wise itermediate products and then build the final archive as a rollup.

Another thing nice observation is, many of these queries are ~100s of ms or faster, even when dealing with a month of data as in the examples above... This means it's feasible to update this daily and likely much more often than that to produce a kind of "latest" view of various data products.
