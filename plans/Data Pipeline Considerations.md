# Data Pipeline Considerations

For the purposes of discussion, we use the terms "long term" to be data files, likely Hive/Parquet, that rarely change and can be considered mostly immutable. The term "short term" reflects continuously arriving data that could see late datagrams or be used for triggering actions, dashboards, etc.

**Long Term:** The canonical path for Sage data is to accumulate and eventually be indexed and compressed for archival storage.  We should consider a few important workflows:

* **Redaction:**  We can all imagine the case where a plugin is reporting data, and later, after the fact, we realize that the data unintentionally published something that should not be in the archives.  Of course, this situation reflects a mistake; we want this sitation to be rare.  The workflow would include: a) a decision to redact (policy).  b) a process for searching the Parquet files for matches, and then reconstructing and building a replacement Parquet file.

* **Late Arrivals:**  We must set an "expiration" for updating the Long Term storage.  It is posisble to imagine a Sage node running for a month or two without networking, and then suddenly "catching up" and attempting to publish.  After the late arrival deadline, we would not attempt to attempt to rebuild Parquet files.  This deadline would also be clearly communicated to the user community -- the Sage data pipeline will reject transmission or storage of datagrams past deadline.  This would free Sage to replicate, archive, and generate DOIs in a very straightforward fashion.

If (policy-based) redaction of existing data is very rare, Parquet files would be very durable.

It is not a requirement that the data interface for long term data be isomorphic to short term data.  It is very common for short term data APIs to be different from long term data APIs -- specifically because the computational and data lake cost models, backend storage systems, and DOIs might be quite different.

## Policy Considerations

* **Throttling:** While not required in an initial implementation, it is important that the new data path be amenable to throttling both cloud and node services.  On a node, a wild plugin that is oversampling and overgenerating data entries (size or speed), should trigger a "too many requests" type of exception.  Similarly, a mistakenly written cloud-side consumer of short term data should not swamp the data server.

* **Data requiring Auth:** We currently have image and audio data that exist under specific policy guidelines (privacy, for example).  At this time, we don't have authorization-controlled time-series data.  However, the feature has been requested.  For example, NEON towers have some plant and animal species they are reluctant to share publically, and in some cases are ***contractually restricted***  (site permit) from reporting.  The NEON site in the Great Smoky Mountains cannot publically share the location of specific species.  While restricting images is already possible, if BioClip identifies American Ginseng (heavily poached in the Smokies), that data should be protected.  How might the new data path support this?

## Operational Considerations

* **Non-Sage Long Term Data Path:** As we have seen from CROCUS and other non-NSF partners, flexibility in the data path could be helpful.  Looking first at Long Term storage, it would be helpful if the new data path could push completed, finalized Parquet files in the Long Term path to operational resources not managed or governed by Sage.   (see recent CROCUS discussion).  *After the successful push of the Long Term storage to another system, operational resonsibility, including search and redaction would end.*  Use Case: Suppose the nwf.org (someone on our Advisory Committee) wants to purchase a node and save the accumulated data for their non-profit.  Could the Long Term data be routed to AWS, or other storage platforms?  

* **Non-Sage Short Term Data Path:** Would a better alternative be to transit **all** short-term non-operational (Sage admin) data to nwf.org's AWS, and the Sage team would have no operational connection to their downstream tools (search, etc?)?  The Advisory Committee asked specifically about our "scale up" plan.  It is also a question that Mike asked -- could a team publish their own data to their own servers -- a polcy-based operational consideration.  Could we scale up with partners by letting them push all their data to their AWS?  Or maybe just Long Term?  We should decide what we might support or not support with the new Data Path.

* **Searching Cached Copies:** For infrastructure such as Pelican and NDP, data caching is a strategy for moving data closer to computing systems.  Can the new Data Path support search tools that are *not* hosted by Sage resources, but rather hosted at cached sites, for example UCSD?  Could *all* search and slice data extraction requests for Long Term data be an API call to NSF resources (like Image Search) rather than hosted on Sage servers?


