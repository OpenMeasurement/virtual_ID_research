# virtual_ID_research

This repository currently containes the initial R&D of the VideoAmp data science team on Virtual ID (VID) assignment to impressions/events table. The VID assignment is a set of algorithm, initially suggested in the paper [Virtual People: Actionable Reach Modeling](https://research.google/pubs/pub48387/).

## Code Structure
We assume the existance of the following two tables. The `df_impressions` table containing the impressions/events and the `df_census` table
containing the total number of people in the product of demographics. The input dataframe `df_impressions` is the impression table containing
the following columns:

| Column(s) | Type | Explanation |
| -: | :-: | :-|
|`user_id`        | String       | The user identifier, which is the device_id, cookie_id, etc.
|`device_type`    | String       | The medium through which the impression is created, e.g. TV/Linear, Digital, etc
|`timestamp`      | Timestamp    | The timestamp of the impression/event occurrence.
|`weight`         | Integer      | The weight associated to the user type is integer. The outcome of panel skew correction.
|`geolocation`    | String       | A column representation geolocation, DMA, or zip code, etc
|`demo_cols`      | String(s)    | A set of columns representing the demographics associated to the user
|`extra_cols`     | 0,1 Integer(s)  |A set of columns of extra labels, such as interest, etc, associated to the user

The `df_census` table contains the following columns:
| Column(s) | Type | Explanation |
| -: | :-: | :-|
| `demo_cols`  | String(s) | A set of columns representing the demographics associated to the user |
| `population` | Integer   | The population size corresponding the combinations of the demographics |

The `demo_cols` columns are only used when in-segment reach curve
is desired, for example when the exact reach curve inside each of the segments
such as `age_range`, `ethnicity`, etc is needed.

All the functions, including the VID assignments, reach/frequency calculations, and plottings functionalities are currenlty under the `vid_spark.py` file.

### Notebooks
Table of available notebooks and a brief description of their content.
| Notebook | Description |
| :-: | :- |
| [VID_uniform_reach](https://github.com/VideoAmp/privacyAmp/blob/master/VID_uniform_reach.ipynb) | Detailed notebook about the issues with assuming uniform reach and comparison with the alternative, in-segment learning of reach curves. |
| [VID_basic_count](https://github.com/VideoAmp/privacyAmp/blob/master/VID_basic_count.ipynb) | The basic VID assignment example. Here we also explain why we have to use a unique impression identifier to assign VIDs. |
| [VID_basic_explode](https://github.com/VideoAmp/privacyAmp/blob/master/VID_basic_explode.ipynb) | The basic VID assignment example where the impressions are exploded based on the associated weight to each user. |
