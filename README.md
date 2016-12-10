Simple backend API for a data query tool.

# About #
Backend using flask with cherrypy server.

# Doco #
## Setup ##
Create a base directory where logging and configuration information can be stored, and put this in `config.py` file.


    BASE_DIR='path/to/local/storage'
    SECRET_KEY=''  # generate this with os.urandom(24)

Run the application with `python
