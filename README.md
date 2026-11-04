# DataScienceRepo
This is a repository for Data Science Projects class 
Data Report
Description & motivation of research questions
Description of our project: We attempted to identify why a bus delay might have happened. There are many different reasons that are possibl, there’s bad weather, possible traffic delays due to something like  a 911 call, or maybe just a slow driver/bus.  
Because transit schedules operate on tight margins, a setback at a single stop often ripples downstream, compounding across the rest of the route. By isolating these different potential causes, our project aims to determine whether a given delay was driven by severe road and weather conditions or normal route dynamics—providing a clearer picture of how and where transit reliability breaks down.
Motivation of our research question:
Most people complain about the bus being late, but a lot of the time, we don’t understand the root cause behind it. We just assume that it’s always gonna be the fault of whoever is driving the bus, that they should just be better and get there on time. But much of the time it’s completely out of their control, whether there’s a traffic delay cause of an accident or bad weather.
A sudden traffic bottleneck, an emergency response corridor, freezing precipitation, or heavy snow can cripple travel times long before a bus reaches its next stop. Because transit schedules run on tight margins, a minor delay at the start of a route quickly compounds downstream, making on-time recovery nearly impossible.
The motivation for this project is to move beyond anecdotal assumptions and quantitatively isolate what actually causes delay. By evaluating bus runs alongside simultaneous roadway congestion and weather conditions, we can distinguish between external environmental friction and internal route bottlenecks. Understanding this distinction is essential not just for fair expectations, but for identifying where infrastructure improvements—like dedicated bus lanes or priority signals—are actually needed. If we see that there are always problems at one specific stop, we can take care of that too, maybe rerouting our bus slightly differently or trying to leave a bit earlier to avoid a consistent rush at the same time. 

Dataset description: 
We created a master parquet out of a few different APIs.  The major one that we took was the Metro Transit GTFS one.  This one took in information about each bus, what time it was observed, what is the route id, the direction it is headed. And our current stop that it is at. It also tells us about the schedule, what time we are supposed to arrive, vs what time the bus actually arrives at the stop, the amount of delay in seconds, and a trip id. Our trip id is important because if our bus faces a delay at one stop, it is more likely than not, this delay will compound. 
	

When it comes to our Open mateo api, there were a few different metrics that we used, all of them pretty self explanatory. We used the temperature in fahrenheit, amount of rain in inches, snow in inches, how windy it is in Mile per hour and the WMO weather code. The WMO stands for the World Meteorological Organization. To interpret the code, Open-Meteo uses a universal number system where each number represents a specific weather condition, like clear skies, rain, fog, or heavy snow. This is important because just knowing the amount of precipitation doesn't tell the whole story—an inch of cold rain affects road traction and bus delays very differently than an inch of snow or freezing ice. The weather code gives our model a direct way to see exactly what kind of storm a bus is driving through so it can better predict when delays will spike.

And for the TomTom API, I pulled in traffic data using four variables: the current speed of traffic (traffic_current_speed_mph), the normal free-flow speed (traffic_free_flow_speed_mph), whether there was an active road closure (traffic_road_closure), and the jam factor (traffic_jam_factor). The jam factor scores roadway congestion on a simple 0 to 10 scale, where 0 means cars are moving completely freely and 10 means traffic is practically at a dead standstill. Comparing live speed to normal speed alongside the jam factor lets us see how bad local bottlenecks really are, and the road closure flag helps explain any sudden detours or huge jumps in delay. Traffic Jam Factor is decided by 1-current Speed/ free flow speed.


 Ethical issues (who may be harmed and who may benefit)
While the operational benefits of this project are straightforward,enabling Metro Transit analysts to pinpoint persistent bottleneck stops where delays occur independent of traffic, weather, or time of day—the pipeline raises critical ethical questions regarding geographic and social equity. By isolating Route 10, the analysis focuses exclusively on a single radial corridor connecting Downtown Minneapolis to the northern suburbs. The Twin Cities transit network, however, serves a far broader, interconnected metropolitan area, encompassing Saint Paul, diverse suburban communities, light rail arteries, and cross-town bus networks. Evaluating delays on one high-profile route risks over-representing its operational challenges while obscuring service reliability issues in unmonitored sectors, potentially directing agency attention and transit optimizations toward a single corridor at the expense of system-wide equity.
I used AI to help enhance my ethical issues paragraph, but all of the ideas I came up with on my own.
Specific Contributions: 
Sohail: Did the work for modeling and Visualizations.
Dooley: Gathered all of the data and did the writeups for this report

AI Statement: I used AI for a bit to help me flesh out my ideas for ethical issues, and also helped me create a better description of our future plan.

This is a link to our project plan
https://docs.google.com/document/d/19zmEkiq4na01Feei5vm2WRqmjUVHPDIDz5y2bfnvaEY/edit?usp=sharing

