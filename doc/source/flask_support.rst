Flask support
=============

The `Split.io <http://split.io/>`_ SDK API Python client works with `Flask <http://flask.pocoo.org/>`_ out of the box. We support with Flask using our `Redis <http://redis.io>`_ backend mentioned in the Redis support section of the :doc:`/introduction`.

The following section shows how to use Split.io in a simple one-view Flask app.

A simple Flask App
------------------

This example assumes that the Split.io configuration is save in a file called ``splitio-config.json``. ::

    import logging
    from flask import Flask, render_template, request

    from splitio import get_redis_client

    logging.basicConfig(level=logging.INFO)

    app = Flask(__name__)

    client = get_redis_client(None, config_file='splitio-config.json')


    @app.route('/')
    def index():
        user = request.args.get('user', '')

        context['some_treatment'] = client.get_treatment(user, 'some_feature')
        context['some_other_treatment'] = client.get_treatment(user, 'some_other_feature',
            {'number_attribute': 42, 'date_attribute': 1466185587010})

        return render_template('index.html', **context)

As long as the update scripts are running periodically, this should work without problems. As mentioned before, if the API key is set to ``'localhost'`` a localhost environment client is generated and no connections to Split.io are made as everything is read from ``.split`` file (you can read about this feature in the Localhost Environment section of the :doc:`/introduction`.)