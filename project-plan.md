# Project plan

## Objective

The objective of our project is to provide analytical datasets from the Spotify API, focusing on new music releases and their associated audio features. This will facilitate deeper insights into music trends and preferences.


## Consumers

The primary users of our datasets are Data Analysts, Music Industry Professionals, and Marketers. They will access the data through a web interface or a dashboard that allows for easy exploration of trends and features related to new music releases.


## Questions

What questions are you trying to answer with your data? How will your data support your users?

Example:

> - What are the most popular new releases each week?
> - How do audio features (e.g., danceability, energy) correlate with track popularity?
> - What genres are most commonly released in new music?
> - How does the release frequency of artists vary over time?
> - Are there seasonal trends in new music releases?
> - How do the audio features of new releases compare across different artists?

## Source datasets

What datasets are you sourcing from? How frequently are the source datasets updating?

Example:

| Source Name           | Source Type | Source Documentation                       |
|----------------------|-------------|-------------------------------------------|
| Spotify New Releases  | REST API   | [Spotify API Documentation](https://developer.spotify.com/documentation/web-api/) |

The Spotify API updates with new releases daily, so we will schedule data extraction to occur at least once a day to capture all new music.

## Solution architecture

How are we going to get data flowing from source to serving? What components and services will we combine to implement the solution? How do we automate the entire running of the solution?

- What data extraction patterns are you going to be using?
- What data loading patterns are you going to be using?
- What data transformation patterns are you going to be performing?

We recommend using a diagramming tool like [draw.io](https://draw.io/) to create your architecture diagram.

Here is a sample solution architecture diagram:

![images/sample-solution-architecture-diagram.png](images/sample-solution-architecture-diagram.png)

## Breakdown of tasks

How is your project broken down? Who is doing what?

We recommend using a free Task board such as [Trello](https://trello.com/). This makes it easy to assign and track tasks to each individual.

Example:

![images/kanban-task-board.png](images/kanban-task-board.png)
