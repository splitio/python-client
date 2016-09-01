Flask support
=============

The `Split.io <http://split.io/>`_ SDK API Python client works with `Flask <http://flask.pocoo.org/>`_ out of the box. Both our in-memory and `Redis <http://redis.io>`_ backed clients work well with Flask.

The following section shows how to use Split.io in a simple one-view Flask app.

A simple Flask App
------------------

This example assumes that the Split.io configuration is save in a file called ``splitio-config.json``. ::

    import logging
    from flask import Flask, render_template, request

    from splitio import get_redis_client, get_client

    logging.basicConfig(level=logging.INFO)

    app = Flask(__name__)

    # With redis
    #client = get_redis_client('SOME-API-KEY', config_file='splitio-config.json')

    # In-memory
    client = get_client('SOME-API-KEY', config_file='splitio-config.json')


    @app.route('/')
    def index():
        user = request.args.get('user', '')

        context['some_treatment'] = client.get_treatment(user, 'some_feature')
        context['some_other_treatment'] = client.get_treatment(user, 'some_other_feature',
            {'number_attribute': 42, 'date_attribute': 1466185587010})

        return render_template('index.html', **context)

When using the Redis client the update scripts need to be run periodically, otherwise there won't be any data available to the client.

As mentioned before, if the API key is set to ``'localhost'`` a localhost environment client is generated and no connections to Split.io are made as everything is read from ``.split`` file (you can read about this feature in the Localhost Environment section of the :doc:`/introduction`.)