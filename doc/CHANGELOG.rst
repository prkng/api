Changelog
=========

WIP
---

- add long/lat coordinates in the checkin reports (admin site)
- remove null values from the agenda view

v1.0.2
------

- use float type instead of string for button_location
- use button_location coordinates for checkins
- remove flask-login for the API

v1.0.1
------

- add a button_location property with the center of parking slot
- Improve database connection handler
- Change API endpoint for slots, coordinates are now handled in query parameters

v1.0.0
------

- first production release

v0.3
----

- refactor Authentication system
- add admin site

v0.2
----

- Add check-in endpoint to the API
- many bug fixes
- improve tests
- Add Authentication using Flask-Login and rauth
    - Google OAuth2
    - Facebook OAuth2

v0.1
----

- Initial release
- Parking free slots for Montréal
- Parking free slots for Québec
