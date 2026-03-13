# Climate Champions Project
Data Engineering project to showcase California state legislators who have sponsored passing climate-related bills in the 2025-2026 session. Uses Legiscan, BillTrack50, and OpenStates APIs and Google Cloud (BigQuery, App Engine, Cloud Functions, Cloud Scheduler). Displays data on a dashboard web app.

## Project Purpose
The purpose of this project is to gain more experience working with key political data APIs ([Legiscan](https://legiscan.com/legiscan), [BillTrack50](https://www.billtrack50.com/documentation/webservices), and [OpenStates](https://docs.openstates.org/api-v3/)), building data pipelines in Python, database development with BigQuery/SQL/Dataform, cloud architecture (Google Cloud Functions, Google Cloud Scheduler), web development (Flask, HTML/CSS/JavaScript, App Engine), and AI development tools (GitHub Copilot).

Note that this project is currently an MVP / proof-of-concept and more work should be done to improve code style, add additional features, and make some things more generalizable.

## Deployed Web Application
A deployed web application is available at [https://climate-project-489910.uw.r.appspot.com/](https://climate-project-489910.uw.r.appspot.com/).

## Data Architecture Overview
- TODO: Add data architecture diagram
- TODO: Add data architecture description
- TODO: Add data architecture analaysis including pros/cons and potential alternatives and improvements

## Database Overview
- TODO: Add database development diagram (e.g. how src, stg, and reporting tables connect, with dependencies)
- TODO: Add database development analysis including pros/cons and potential alternatives and improvements

## Political Data APIs Discussion (Legiscan, BillTrack50, OpenStates)
- TODO: Add discussion of the pros/cons of each API and why you chose to gather certain data fields from each API.

## Further Work To Be Done
- Integrate Vertex AI / Gemini model to analyze whether or not Climate Cabinet would approve each climate-related bill
- Add functionality to find Climate Champion Legislators based on any climate-related bills introduced (without the requirement of passage)
- Generalize code and dashboard from California to All States
- Generalize code and dashboard from 2025-2026 Session to All Available Sessions
- Deploy Python code to Cloud Functions
- Set up Cloud Scheduler to run Cloud Funtions at regular intervals
- Convert SQL scripts to Dataform SQLX models with appropriate dependencies and scheduling
- Create Demo Video