from django.db import models

class Order(models.Model):
    id = models.CharField(max_length=100,primary_key=True)
    created = models.DateTimeField()
    imported = models.DateTimeField(auto_now_add=True, auto_now=False)
    
    def __str__(self):
        """
        """
        return self.id