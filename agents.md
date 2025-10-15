The task the night person does is to look at all the tails flying for the next day, then split it up as evenly as possible between the present shifts.
I already have my API data that could be grabbed on demand (I think on a button click). This would then give the number of legs for the next day.
  This would have to be set up to correctly pull flights for the right time period, taking into consideration UTC time conversions and the like.
I would then want the ability to say how many people are in to know how many people the tails have to be split to. Let's say there are 4 people on, the early shift, next shift, next shift, last shift.

Based on the initial timezone of the first flight per day, I would want these dolled out in as even as fashion to each person, though keeping all legs from a single tail on assigned to one person.
  The earliest shift should want a higher preference for the eastern timezones, shifting to a preference for western timezones for the later shifts, however still keeping them even as the most important part.

  I would want to set up the same API access that I have set up in my FF Dashboard app, however rather than a continuous poll, this would just need a button that would do a single pull on press, which would then provide all of the flights to then be assigned. This would also use the GET to have crew information pulled.
  Once the flights have been assigned, I would then set up a webhook in telus business connect so that the sorted information could be sent over to the daily flight sheet chat.
