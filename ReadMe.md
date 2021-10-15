<h1>DATA PIPELINE EXAMPLE:</h1>

In this example, we're collecting financial statements publicly available from the financial regulation authority
of Colombia. After fetching all the required information, we're making a simple data cleaning and pushing it to an
either a relational (either local or server) database.

All the data can be manually collected from: https://www.superfinanciera.gov.co/jsp/index.jsf

It is required that to run this example, create environment variables with all the credentials required to connect
to your database.

This example uses sqlalchemy, so please refer to this website to check how to connect to your server.

https://docs.sqlalchemy.org/en/14/core/engines.html#mysql
