# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Order',
            fields=[
                ('id', models.CharField(serialize=False, max_length=100, primary_key=True)),
                ('created', models.DateTimeField()),
                ('imported', models.DateTimeField(auto_now_add=True)),
            ],
        ),
    ]
