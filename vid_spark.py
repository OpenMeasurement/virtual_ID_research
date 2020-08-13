import numpy as np
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', 1000)

import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

from scipy.optimize import curve_fit
from numpy.random import random

import pyspark.sql.functions as F
import pyspark.sql.types as spark_types
from pyspark.sql import Window
from pyspark import StorageLevel
from pyspark.sql.types import ArrayType, IntegerType, StringType


##################################################
### reach and frequency calculations from data ###
##################################################

def add_exposure_index(df_impressions, id_col="user_id", order_col="timestamp") :
    """
    Calculate the exposure_index for each impression. The `exposure_index` is a integer `n > 0`
    which indicates the corresponding impression is the `n`th time the user has been exposed. This
    is used in reach/frequency calculations.

    The table is partitioned by `id_col` and ordered by `timestamp`.
    """
    if id_col not in df_impressions.columns :
        raise Exception(f"The id_col {id_col} does not exist!")
    if order_col not in df_impressions.columns :
        raise Exception(f"The order_col {order_col} does not exist!")
    
    return df_impressions.withColumn("exposure_index", F.row_number()
            .over(Window.partitionBy(id_col).orderBy(order_col)))

def add_frequency_total(df_impressions, id_col="user_id", order_col="timestamp") :
    """
    Calculate the `frequency_total` that is the frequency of exposure for each unique `id_col`.
    This method also adds the `exposure_index` (1 to `frequency_total`) in case needed for 
    other calculation.
    The table is partitioned by `id_col` and ordered by `timestamp`.
    """
    if id_col not in df_impressions.columns :
        raise Exception(f"The id_col {id_col} does not exist!")
    if order_col not in df_impressions.columns :
        raise Exception(f"The order_col {order_col} does not exist!")
    
    window = Window.partitionBy(id_col)
    df = (
        df_impressions
        .withColumn("exposure_index",  F.row_number().over(window.orderBy(order_col)))
        .withColumn("frequency_total", F.count("*").over(window))
    )
    return df 
    
def generate_reach_table(df_impressions, id_col="user_id", demo_cols=None, mode="count") :
    """
    The reach is calculated assuming the `df_impressions` is a spark dataframe
    by first calculating the an `exposure_index` (an integer representing the `n`th 
    time the user is exposed) and then filtering only the by the `exposure_index=1`.

    The `mode` parameter takes two possible values, "weighted", and "count". Where
    the "weighted" reach calculated the weighted reach and impression and the 
    "count" simply count the total number of impressions.

    The output is a table containing the columns
    `timestamp`.      : The timeslot of the ad.
    `reach_inc`       : incremental unique person reach for this time slot.
    `impression_inc`  : incremental number of impressions for this time slot.
    `reach`           : total unique person.
    `impression`      : total number of impressions.
    """
   
    # add the "is_first column" for 1+_reach calculation
    select_cols = [id_col, "is_first", "timestamp"]
    if demo_cols is not None :
        select_cols = select_cols + demo_cols  
    df = (
        add_exposure_index(df_impressions, id_col=id_col)
        .withColumn("is_first", F.when(F.col("exposure_index")==1, 1).otherwise(0))
        .select(*select_cols)
    )

    if demo_cols is not None:
        if mode == "count" :
            return _generate_reach_table_count_in_segments(df, demo_cols=demo_cols)
        elif mode == "weighted" :
            return _generate_reach_table_weighted_in_segments(df, demo_cols=demo_cols)
        else:
            raise Exception(f"unexpected mode {mode}")
    
    else :
        if mode == "count" :
            return _generate_reach_table_count(df)
        elif mode == "weighted" :
            return _generate_reach_table_weighted(df)
        else :
            raise Exception(f"unexpected mode {mode}")
       
   
def _generate_reach_table_count(df) :
    window1 = Window.partitionBy(F.col("timestamp"))
    window2 = Window.orderBy(F.col("timestamp"))
    return (
        df
        .withColumn("reach_inc", F.sum("is_first").over(window1))
        .withColumn("impression_inc", F.count("*").over(window1))

        .select("timestamp", "reach_inc", "impression_inc").distinct()

        .withColumn("reach", F.sum(F.col("reach_inc")).over(window2))
        .withColumn("impression", F.sum(F.col("impression_inc")).over(window2))
    )


def _generate_reach_table_weighted(df) :
    window1 = Window.partitionBy(F.col("timestamp"))
    window2 = Window.orderBy(F.col("timestamp"))
    return (
        df  
        .withColumn("reach_inc", F.sum(F.col("weight") * F.col("is_first")).over(window1))
        .withColumn("impression_inc", F.sum("weight").over(window1))

        .select("timestamp", "reach_inc", "impression_inc").distinct()

        .withColumn("reach", F.sum(F.col("reach_inc")).over(window2))
        .withColumn("impression", F.sum(F.col("impression_inc")).over(window2))
    )

def _generate_reach_table_count_in_segments(df, demo_cols):
    window1 = Window.partitionBy(F.col("timestamp"), *[F.col(c) for c in demo_cols])
    window2 = Window.orderBy(F.col("timestamp"))
    window3 = Window.partitionBy(*[F.col(c) for c in demo_cols]).orderBy(F.col("timestamp"))
    return (
        df       
        .withColumn("reach_inc", F.sum("is_first").over(window1))
        .withColumn("impression_inc", F.count("*").over(window1))

        .select(*demo_cols, "timestamp", "reach_inc", "impression_inc").distinct()

        .withColumn("reach", F.sum(F.col("reach_inc")).over(window3))
        .withColumn("impression", F.sum(F.col("impression_inc")).over(window2))
    )

def _generate_reach_table_weighted_in_segments(df, demo_cols):
    window1 = Window.partitionBy(F.col("timestamp"), *[F.col(c) for c in demo_cols])
    window2 = Window.orderBy(F.col("timestamp"))
    window3 = Window.partitionBy(*[F.col(c) for c in demo_cols]).orderBy(F.col("timestamp"))
    return (
        df     
        .withColumn("reach_inc", F.sum(F.col("weight") * F.col("is_first")).over(window1))
        .withColumn("impression_inc", F.sum("weight").over(window1))

        .select(*demo_cols, "timestamp", "reach_inc", "impression_inc").distinct()

        .withColumn("reach", F.sum(F.col("reach_inc")).over(window3))
        .withColumn("impression", F.sum(F.col("impression_inc")).over(window2))
    )

def generate_frequency_table(df_impressions, id_col="user_id", demo_cols=None, mode="count") :
    """
    The frequency table is calculated assuming the `df_impressions` is a spark dataframe
    by first calculating the an `exposure_index` (an integer representing the `n`th 
    time the user is exposed) and then filtering only by the largest the `exposure_index=1`.

    The `mode` parameter takes two possible values, "weighted", and "count". Where
    the "weighted" reach calculated the weighted reach and impression and the 
    "count" simply count the total number of impressions.

    The output is a table containing the columns        
    """
   
    # add the "is_first column" for 1+_reach calculation
    select_cols = [id_col, "frequency_total"]
    if demo_cols is not None :
        select_cols = select_cols + demo_cols
    
    groupby_cols = ["frequency_total"]
    if demo_cols is not None :
        groupby_cols = demo_cols + groupby_cols
   

    df = (
        add_frequency_total(df_impressions, id_col=id_col)
        .select(*select_cols).distinct()
        .groupby(*groupby_cols)
        .agg(F.count("*").alias(f"{id_col} counts"))
        .sort(*groupby_cols)
    )
    return df 

@F.udf(returnType=ArrayType(StringType()))
def udf_explode_user_id(n, user_id) :
    return [str(user_id) + str(i) for i in range(1, n+1)]
    
def explode_weights_to_impressions(df_impressions) :
    columns = df_impressions.columns
    columns.remove("user_id")
    return (
        df_impressions
        .withColumn("user_ids", udf_explode_user_id(F.round("weight").astype("int"), F.col("user_id")))
        .withColumn("new_user_id", F.explode(F.col("user_ids")))
        .select(F.col("new_user_id").alias("user_id"), *columns)
    )

#####################################
### The mixture of delta modeling ###
#####################################
                        
def delta_mixture_reach(g, *ps) :
    """
    The delta mixture reach curve as a function of impression/event count. This is
    \[
        R(g) = \sum_i \alpha_i (1 - \exp(-x_i g))
    \]
    where $\alpha_i$ is the amplitude of pool $i$ and $x_i$ is the rate
    of cookie generation (impression receival) of the corresponding pool.
    The number of pools is inferred from the size of the parameter list `ps`. It should 
    be a `2n` list of alphas (amplitudes) followed by xs (rates).
    """
    if len(ps) % 2 != 0 :
        raise Exception("The length of parameters should be an even number (one amplitude and one rate for each delta)")
    
    n = len(ps) // 2
    alphas = np.array(ps[:n])
    rates = np.array(ps[n:])

    return np.dot(alphas, (1 - np.exp(np.outer(-rates, g))))
                        
def fit_mixture_of_deltas(df_reach, population_size, n_deltas=3, add_zero=True) :
    """
    learn the reach curve as a function of grp(g). This fits a mixture of delta functions to data.
    `n` is the number of deltas.
    `data` is Tx2 array (where T is the total number of data points).
    
    returns (alphas, rates)
    """
    data = df_reach[["impression", "reach"]].toPandas().to_numpy()
    data = np.vstack([[0, 0], data])
    alphas, rates = _fit_mixture_of_deltas(data/population_size,
                                           n_deltas,
                                           verbose=True, 
#                                            bounds=([0.0] * n_deltas + [0.1]    * n_deltas,  
#                                                    [1.0] * n_deltas + [np.inf] * n_deltas),
                                           method="basic")
    return alphas, rates

                        
def initparams_mixture_of_deltas(n, rate_upperbound=6) :
    """
    Generates random initial parameters for n mixture of deltas. 
    
    The amplitudes are normalized and for the descendingly sorted rates, the amplitudes
    are descending. That is because we generally except people how generate more frequently
    are less populated.
    """
    alphas_0 = random(n)
    alphas_0 /= np.sum(alphas_0)
    alphas_0.sort()
    rates_0 = random(n) * rate_upperbound
    rates_0.sort()
    return [*alphas_0[::-1], *rates_0]
                        
## TODO: extend the below function and add more fitting capabilities.
def _fit_mixture_of_deltas(data, n=3, verbose=False, bounds=(0, np.inf), method="basic") :
    alphas = np.zeros(n)
    rates = np.zeros(n)
    jump = 1
    m = n
    while True:
        try: 
            ps, ps_cov = curve_fit(delta_mixture_reach, 
                                   data[1::jump, 0], 
                                   data[1::jump, 1], 
                                   p0=initparams_mixture_of_deltas(m), 
                                   bounds = bounds, 
                                   maxfev=5000)

            if m < 2 or all(ps[m:] > 0.01) :
                alphas[n-m:n] = ps[:m]
                rates [n-m:n] = ps[m:]
                if verbose :
                    print("[verbose] ", end="")
                    print("alphas : ", np.around(alphas, 4), end=", ")
                    print("rates  : ", np.around(rates, 4))
                return alphas, rates
            else :
                m = m - 1
        except Exception as e:
            print(e)
            if m == 1 :
                raise Exception("Even one delta didn't fit!")
            m = m - 1
                        
                        
def plot_reach_curve(df_reach, population_size, add_zero=True, alphas=None, rates=None, 
                     plot_deltas=False, reach_kwargs={}) :
    """
    Plots the reach curve from the `df_reach` table containing reach and impressions with
    timestamps which is used for ordering. If `alphas` and `rates` of the dirac mixtures are
    provided the fit is also plotted, and the dirac deltas are also shown as arrow.
    """
    data = df_reach[["impression", "reach"]].toPandas().to_numpy()
    data = np.vstack([[0, 0], data])
    if plot_deltas :
        fig, axs = plt.subplots(1, 2, figsize=(10,4))
        plot_dirac_mixtures(alphas, rates, axes=axs[1])
    else :
        fig, ax = plt.subplots(figsize=(8,6))
        axs = [ax]
  
    axes_info = {'label' : "full reach"}
    if 'axes_info' in reach_kwargs :
        axes_info = reach_kwargs['axes_info']
        
    jump = 1
    if 'jump' in reach_kwargs:
        jump = reach_kwargs['jump']
    axs[0].plot(data[::jump, 0], data[::jump, 1], **axes_info)
    axs[0].set_title(f"reach_curve")
    
    
    ylim = (0, 1.0e8)
    if 'ylim' in reach_kwargs :
        ylim = reach_kwargs['ylim']
        
    axs[0].set_ylim(*ylim)
    axs[0].set_xlim(0, max(data[:, 0]))
    
    if alphas is not None and rates is not None:
        axs[0].plot(data[:, 0], 
                delta_mixture_reach(data[:, 0]/population_size, *alphas, *rates) * population_size, 
                'C3--', label = f"fit:  ${100*np.around(np.sum(alphas), 3):.1f}\%$ of population")
    axs[0].legend()
    return fig, axs

def plot_dirac_mixtures(alphas, rates, axes=None, **kwargs) :
    if axes is None:
        fig, ax = plt.subplots(**kwargs)
    else :
        ax  = axes
        fig = plt.gcf()
    for i, x in enumerate(rates):
        ax.arrow(x, 0, 0, alphas[i], color='C3', width=0.1, head_length=0.03)
        ax.set_xlim(0, max(rates) * 1.2)
    ax.set_title("The mixture of deltas")
    return fig, ax

def penetrate_uniform_reach(df_census, alphas, rates, demo_cols) :
    population_size = df_census.agg(F.sum("population")).collect()[0][0]
    df_dirac_mixtures = (
        df_census
        .groupBy(*demo_cols).agg((F.sum("population")/population_size).alias("ratio"))
        .withColumn("alphas", F.array([F.round(F.col("ratio") * alpha, 8) for alpha in alphas]))
        .withColumn("rates", F.array([ F.round(F.lit(rate), 8) for rate in rates]))
        .sort(*demo_cols)
        .select(
            *demo_cols, 
            "ratio",
            "alphas",
            "rates"
        )
    ).toPandas()
    return df_dirac_mixtures

# Extending the matplotlib for 3D arrows 
from matplotlib.patches import FancyArrowPatch
from mpl_toolkits.mplot3d.proj3d import proj_transform
from mpl_toolkits.mplot3d.axes3d import Axes3D

class Arrow3D(FancyArrowPatch):
    def __init__(self, x, y, z, dx, dy, dz, *args, **kwargs) :
        super().__init__((0,0), (0,0), *args, **kwargs)
        self._xyz = (x,y,z)
        self._dxdydz = (dx,dy,dz)

    def draw(self, renderer):
        x1,y1,z1 = self._xyz
        dx,dy,dz = self._dxdydz
        x2,y2,z2 = (x1+dx,y1+dy,z1+dz)

        xs, ys, zs = proj_transform((x1,x2),(y1,y2),(z1,z2), renderer.M)
        self.set_positions((xs[0],ys[0]),(xs[1],ys[1]))
        super().draw(renderer)
        
def _arrow3D(ax, x, y, z, dx, dy, dz, *args, **kwargs):
    arrow = Arrow3D(x, y, z, dx, dy, dz, *args, **kwargs)
    ax.add_artist(arrow)

setattr(Axes3D,'arrow3D',_arrow3D)

def dirac_mixtures_plot_3d_segment(df_dirac_mixtures, demo_cols, ax=None, color='C3') :
    if ax is not None :
        fig = plt.gcf()
    else : 
        fig, ax = plt.subplots(subplot_kw=dict(projection='3d'))
    
    max_rate = 0
    max_alpha = 0
    for i in df_dirac_mixtures.index:
        label = df_dirac_mixtures[demo_cols].loc[i]
        alphas = df_dirac_mixtures['alphas'].loc[i]
        rates = df_dirac_mixtures['rates'].loc[i]
        for j, x in enumerate(rates):
            ax.arrow3D(x, i, 0, 0, 0, alphas[j], color=color, mutation_scale=10, arrowstyle="-|>")
        if np.max(rates) > max_rate :
            max_rate = np.max(rates)
        if np.max(alphas) > max_alpha:
            max_alpha = np.max(alphas)
    
    ax.set_ylim(0, df_dirac_mixtures.shape[0])
    #ax.set_xlim(0, max_rate  * 1.2)
    ax.set_xlim(0, 2.0)
    ax.set_zlim(0, max_alpha * 1.1)
    
    ax.set_xlabel("rates")
    #ax.set_ylabel(segment)
    ax.set_zlabel("amplitudes")
    ax.grid(False)
    ax.set_xticks(range(int(max_rate)))
    yticks = map(lambda x : ",".join(x), df_dirac_mixtures[demo_cols].to_numpy())
    ax.set_yticklabels(yticks, rotation=-30)
    return fig, ax
                        
def plot_frequency(frequency, axes=None, id_col="user_id", n_bars=20, **kwargs) :

    dp = frequency.limit(n_bars).toPandas()
    if axes is None:
        fig, axes = plt.subplots()
        
    xs_bars = np.arange(n_bars) + 1
    axes.bar(xs_bars, dp[f"{id_col} counts"], alpha=0.5, **kwargs, label=f"{id_col} counts")
    
    axes.set_xlabel("Frequency")
    axes.set_ylabel("Population count")
    axes.set_ylim(0)
    axes.set_xlim(0)
    axes.set_xticks(xs_bars)

    axes.legend()
    return axes

################################
### VID assignment functions ###
################################
                        
                
def generate_vid_assignment_info_per_pool(alphas, rates, previous_vid, population_size):
    """
    Returns probability, alphas(amplitudes), rates, start_VID, total_VID for the each pool. 
    There are
    """
    aks = alphas * rates
    kappa = np.sum(aks)

    N_ks  = np.ceil(alphas * population_size)
    N_ks_cumsum = np.insert(np.cumsum(N_ks), 0 , 0) + previous_vid
    end_vid = N_ks_cumsum[n]
    return [*aks/kappa, *alphas, *rates, *N_ks_cumsum[:n]+1, *N_ks], end_vid

def generate_vid_assignment_table(dirac_mixtures, population_size, demo_cols=None, normalize=False) :
    """
    The input dirac_mixtures is a Pandas dataframe, a set of delta functions over rates of cookie generation. 
    This method generates the range of Virtual ID for each pool of people (in-segment) and
    the probability bounds of belonging to that pool, the probability bounds (bucket) is a
    range $\in [0, 1)$.
    
    Returns a Pandas table, consisting of the `demo_cols` columns followed by two columns
    for the probability bucket `prob_>=` and `prob_<`, followed by `alpha` and `rate` and
    two columns for the range of VIDs `start_VID`, `total_VID`.
    """
    n = len(dirac_mixtures['alphas'][0])
    if demo_cols is None :
        dp = pd.DataFrame(columns=['prob_>=', 'prob_<', 
                                   'alpha', 'rate', 
                                   'start_VID', 'total_VID'])
        previous_vid = 0
        alphas = np.array(dirac_mixtures['alphas'].loc[0])
        rates  = np.array(dirac_mixtures['rates' ].loc[0])

        aks = alphas * rates
        kappa = np.sum(aks)

        N_ks  = np.ceil(alphas * population_size)
        N_ks_cumsum = np.insert(np.cumsum(N_ks), 0 , 0) + previous_vid
        previous_vid = N_ks_cumsum[n]
        cum_prob = np.insert(np.cumsum(aks), 0, 0)
        if normalize :
            cum_prob = cum_prob / kappa
        for j in range(n):
            dp.loc[j]  = [cum_prob[j], cum_prob[j+1], 
                          alphas[j], rates[j], 
                          int((N_ks_cumsum[:n]+1)[j]), int(N_ks[j])]
        if not normalize :
            dp.loc[n] = [cum_prob[n], 1.0, 0., 0., 0, 1]

    else :
        dp = pd.DataFrame(columns=[*demo_cols, 
                                   'prob_>=', 'prob_<', 
                                   'alpha', 'rate', 
                                   'start_VID', 'total_VID'])
        previous_vid = 0
        for i in dirac_mixtures.index: 
            demos  = dirac_mixtures[demo_cols].loc[i]
            alphas = np.array(dirac_mixtures['alphas'].loc[i])
            rates  = np.array(dirac_mixtures['rates' ].loc[i])

            aks = alphas * rates
            kappa = np.sum(aks)

            N_ks  = np.ceil(alphas * population_size)
            N_ks_cumsum = np.insert(np.cumsum(N_ks), 0 , 0) + previous_vid
            previous_vid = N_ks_cumsum[n]
            cum_prob = np.insert(np.cumsum(aks/kappa), 0, 0)
            for j in range(n):
                dp.loc[n*i+j]  = [*demos, 
                                  cum_prob[j], cum_prob[j+1], 
                                  alphas[j], rates[j], 
                                  int((N_ks_cumsum[:n]+1)[j]), int(N_ks[j])]
    
    dp = dp.astype({'prob_>='   : float,
                    'prob_<'    : float,
                    'alpha'     : float,
                    'rate'      : float,
                    'start_VID' : int,
                    'total_VID' : int})

    return dp



@F.udf(returnType=ArrayType(IntegerType()))
def udf_find_n_vids(n, start, size, seed) :
    "The user defined function to generate `n` VIDs in the range based on the initial seed (hashing recursively)"
    if n < 1 :
        return []
    m = 0
    h = seed
    vids = np.zeros(n, dtype=int)
    vids[0] = hash(str(h)) % size + start
    #vids[0] = np.int64((hash(str(h)) % (2**32)) / (2**32) * size) + start
    for i in range(1, n):
        vids[i] =  hash(str(vids[i-1])) % size + start
        #vids[i] = np.int64((hash(str(vids[i-1])) % (2**32)) / (2**32) * size) + start
    return vids.tolist()

def assign_vids(df_impressions, vid_assignment_table, population_size, demo_cols=None, mode="uid") :
    """
    Assigns virtual id (VID) to impression tables. The inputs are
    An impression table `df_impressions`, 
    A vid assignement table with probability and VID ranges based on the mixture_of_deltas `vid_assignment_table`.
    The demographic columns relevant for this assignment.
    The total population size.
    If the parameter `weighted` is set to true then there will n different VIDs created
    for each user_id, where n is the (rounded) associated weight to that user.
    """
    
    if mode == "weighted" :
        return assign_vids_weighted(df_impressions, vid_assignment_table, demo_cols)
    elif mode == "uid" :
        return assign_vids_uid(df_impressions, vid_assignment_table, demo_cols)
    elif mode == "uid_time" :
        return assign_vids_uid_time(df_impressions, vid_assignment_table, demo_cols)
    else :
        raise Exception(f"unexpected mode {mode}")

def assign_vids_weighted(df_impressions, vid_assignment_table, demo_cols=None) :
    select_cols = ["vid", "timestamp", "h1", "p1"]
    if demo_cols is not None:
        df = df_impressions.join(F.broadcast(vid_assignment_table), demo_cols)
        select_cols = demo_cols + select_cols
    else :
        df = df_impressions.crossJoin(F.broadcast(vid_assignment_table))
        
    df_vid_impressions = (
        df
        .withColumn("h1", F.hash(F.col("user_id").astype("string")))
        #.withColumn("h1", F.hash("user_id"))
        .withColumn("p1", F.col("h1") / (2**32) + 0.5 )
        .withColumn("n", F.round("weight").cast("integer"))

        .where(F.col("p1") >= F.col("prob_>="))
        .where(F.col("p1") <  F.col("prob_<" ))

        .withColumn("vids", udf_find_n_vids(F.col("n"), F.col("start_VID"), F.col("total_VID"), F.col("h1")))
        .withColumn("vid", F.explode(F.col("vids")))
        
        .select(*select_cols)
    )
    return df_vid_impressions

def assign_vids_uid(df_impressions, vid_assignment_table, demo_cols) :
    select_cols = ["vid", "timestamp", "h1", "p1", "p2", "user_id"]
    if demo_cols is not None:
        df = df_impressions.join(F.broadcast(vid_assignment_table), demo_cols)
        select_cols = demo_cols + select_cols
    else :
        df = df_impressions.join(F.broadcast(vid_assignment_table))
    
    df_vid_impressions = (
        df
        .withColumn("h1", F.hash(F.col("user_id").astype("string")))
        #.withColumn("h1", F.hash("user_id"))
        .withColumn("p1", F.col("h1") / (2**32) + 0.5 )

        .where(F.col("p1") >= F.col("prob_>="))
        .where(F.col("p1") <  F.col("prob_<" ))
 
        .withColumn("p2", F.hash(F.col("h1").astype("string")) / (2**32) + 0.5)
        .withColumn("vid", (F.col("p2") * F.col("total_VID")).astype('int') + F.col("start_VID"))
        
        .select(*select_cols)
    )
    return df_vid_impressions

def assign_vids_uid_time(df_impressions, vid_assignment_table, demo_cols) :
    select_cols = ["vid", "timestamp", "h1", "p1", "p2", "user_id"]
    if demo_cols is not None:
        df = df_impressions.join(F.broadcast(vid_assignment_table), demo_cols)
        select_cols = demo_cols + select_cols
    else :
        df = df_impressions.join(F.broadcast(vid_assignment_table))
    
    df_vid_impressions = (
        df
        .withColumn("h1", F.hash(F.concat(F.col("user_id").astype("string"), 
                                          F.col("timestamp").astype("string"))))
        #.withColumn("h1", F.hash("user_id"))
        .withColumn("p1", F.col("h1") / (2**32) + 0.5 )

        .where(F.col("p1") >= F.col("prob_>="))
        .where(F.col("p1") <  F.col("prob_<" ))
 
        .withColumn("p2", F.hash(F.concat(F.col("h1").astype("string"), 
                                         F.col("timestamp").astype("string"))) / (2**32) + 0.5)
        .withColumn("vid", (F.col("p2") * F.col("total_VID")).astype('int') + F.col("start_VID"))
        
        .select(*select_cols)
    )
    return df_vid_impressions

def mixture_of_delta_pool_info(vid_impressions, vid_assignment_table) :
    """
    Returns the pool information for each delta pool.
    """
    df = (
        vid_impressions
        .join(F.broadcast(vid_assignment_table))
        .where(F.col("vid") >= F.col("start_VID"))
        .where(F.col("vid") <  F.col("start_VID") + F.col("total_VID"))
        .groupby("prob_>=", "prob_<", "alpha", "rate", "start_VID", "total_VID")
        .agg(
            F.count("*").alias("impressions"), 
            F.countDistinct("vid").alias("VID_reach"),
            F.countDistinct("user_id").alias("distinct_users")
        )
        .sort("start_VID")    
        .withColumn("alpha * rate", F.col("alpha") * F.col("rate"))
        .select(
            "alpha",
            "rate",
            "alpha * rate",
            F.col("total_VID").alias("VID_pool_size"),
            "impressions",
            "VID_reach",
            "distinct_users"
        )
    )
    return df 