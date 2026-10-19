Design Ideas for Data Pipeline v2
Intro
We’ve run into a number of problems with our current pipeline, at a high level, I’d say:
InfluxDB is not handling the volume and cardinality of data we collect and no easy or obvious way to fix that.
InfluxDB does not play well with high frequency data collection use cases like CROCUS using the WXT, both for ingesting or querying.
InfluxDB does not support rich data types and especially tabular data. The tabular case feels so common that it feels like we should support that as a first class thing - otherwise, we are constantly asking users to split / rejoin data when it needs to be used together.
InfluxDB doesn’t provide a great long term archive story, which we need anyway.
Data is not discoverable in any real way and there is no real tracked notion of what app publishes what kind of data.
Permissions are hard to implement on top of timeseries data.
Currently don’t have a great way to “share” timeseries data for things like rollups, summaries, or captions. (Julia and Song have an example which could use this.)

I’ll outline a couple potential paths we can take.
Minimal changes to existing pipeline path
There are a lot of things we could do and they would obviously be a huge undertaking and may not be doable or worth taking on given that we only have a few years on SGT. I’ve outlined a much more complete design in the “More complete v2 data pipeline” section.

Instead, an intermediate option is to still try to retain InfluxDB but take the following steps:
Redirect system metrics to the Prometheus / Grafana pipeline. This is by far one of the largest data sources we have and is more or less unused beyond the instantaneous view in the portal.
It might be good to keep one metric (e.g., sys.uptime) for checking on influx pipeline (and WES)
Check how GPS is published
Drop its retention window to something like 6 months
Stop sending high frequency data to InfluxDB.
Investigate reducing cardinality of data published. (Potentially much harder and requires reworking apps. There’s no simple way to just unindex tags while retaining the records we serve.)
Work towards a separate archival process.

I’d say first, we spin up a copy of InfluxDB and check if just the retention window drop solves the problem. Then, we stop the high frequency publishers. If this doesn’t fix things, then we can look into data cardinality and evaluate how much work that is. Ideally, one or both of these solves the InfluxDB problem.

On the archival side, we can choose a subset of the most important data for us, backdefine schemas for it, build an ETL pipeline to structure them into Parquet files for Pelican (structural details are in the more complete v2 pipeline section).

Unfortunately, we are missing important metadata in some places, so we can’t easily group data by “runs” but we can probably approximate this pretty well.

I can chat with Bhupendra again who I advised on how to do this with the CROCUS WXT and AQT data. This has apparently been working very well and he is amazed at how much faster it is to access and run queries across all their data when organized this way.

I would recommend this approach as an initial exploration anyway, to get a feel for the general variety of data shapes we end up with. This also allows us to explore whether the directory structure is sufficient for our permissions and access needs.

This has the advantage that it’s just additive and we don’t need to change the existing pipeline, other than reducing the retention time. Existing apps will continue to work, no data needs to be ported. We will have a zoo of archivers for different apps… but that is almost certainly less work once the basic structure is in place and definitely a place where agents can help the team do that work.

The focus here would be on preserving access to useful data rather than everything.

If this exploration looks promising, I have a more complete v2 data pipeline sketch in the following section.
More complete v2 data pipeline
The new concepts
I think we can address most of these with a single redesign. At a high level, I’m proposing the following new data paths.

Published data will now require real schemas that are tracked as part of the app repo / container image.

This was something I hesitated to do to make development easier but in retrospect, it would solve a bunch of problems at the same time.

It would allow discovery and search of all the kinds of data we publish.
It would allow explicit validation of the type and shape of the data published by the app.

Bhupendra and I chatted about this and he said the environmental science community went through a similar transition and maintains schema files like this, including metadata like the min / max valid ranges to allow validation beyond just types.

I initially hesitated on requiring this but I think with some tooling, this is doable. Here’s what I’m thinking…

When someone is developing an app initially and a new kind of data is published, an initial schema is generated with basic names + types and then the user is asked to edit and verify it instead of having to write the entire thing themselves allowing them to focus on just the bits where we need their input.

There will be one data publish path which tracks the dense storage and the latest state.

Perhaps most importantly, we will move towards bulk data being stored as literal files on disk which can be stored long term on Pelican.

We seem to have at least two different needs. First, driving the real time data for the portal and, second, maintaining a rich long term history. We’ve been supporting both with the same InfluxDB backend but that’s starting to fall over. In particular, this doesn’t do well with high frequency data and that also doesn’t add much to simple charts in the portal.

To address this, I’m proposing the collector does two things:
Batches and writes out Parquet files based on the provided schemas for bulk upload and archival.
Maintains a “latest state” table across all measurements which we control when to sync. (This is heavily inspired by monitoring tools like Prometheus.)

This allows a relatively clean split of highly efficient batching and compression for bulk data movements and allows us to dial up or down how often we sync the latest data view.

In particular, this would have addressed certain data needs for CROCUS - we could have done the bulk data movement but still kept an up to date portal without bogging it and InfluxDB down with their 10Hz tabular measurements from the WXT and AQT.

This also has a nice side effect that the node can build on this to maintain a bulk local data history as well as a simple “latest state”. We’d have to look at how to shape this but at least the data will be there.

I’m also looking at how we can further simplify the bulk path to use a single mechanism for the batched Parquet files and BLOBs. This could make data management potentially much easier.

Some work should go into syncing the latest state but there are a lot of mechanisms. Obviously, batching more data will allow better naive compression. Additionally, we can do a full sync periodically and then just the diffs. I suspect when done at a controlled rate, data costs will be low.

For the local Parquet batching, we will also run a collector which combines files into larger size chunks to balance local reusability against having many small files.

Since we’re introducing some amount of latency by batching and sampling the latest state - this might indicate the need for a separate events pipeline (which may have actually been needed now but was never really articulated.) This would allow urgent events to immediately go to the cloud so they can be acted upon. Perhaps we start simple and these are even just free form logs for the app developer to handle.

Finally, we need to think about the data organization structure. I want to leave it up to the users to search and merge data. Before, I’d hoped people might occupy a global shared data space… but that seems to require way too much discipline and in some cases is not even obvious how to do it correctly. It also makes permissions very hard to enforce in bulk.

Instead, I’d like to essentially isolate all data and then organize it roughly as Hive structured layout:

/run-id=/app-id=/data-schema-id=/node-id=/date=/…parquet files…

or

/run-id=/app-id=/data-schema-id=/node-id=/year=/month=/day=/…parquet files…

This allows relatively quick scoping to just relevant apps and a separate schema index allows users to find relevant apps to look at.

The addition of a run-id (an actual scheduled run of potentially multiple apps x nodes) allows users to share the results of an entire run. The nested app underneath allow perhaps a sensitive component like image sampling to be hidden while the rest is shared too.

In addition, the primary “identity” of data will now be app-id + data-schema-id. This prevents collisions like before but also dramatically simplifies the names people need to give to measurements. Users can literally publish tabular data with fields like temp, pressure, rh.
