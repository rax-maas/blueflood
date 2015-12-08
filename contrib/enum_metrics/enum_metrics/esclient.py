from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError


class ESClient:
    def __init__(self, es_params):
        self.es = Elasticsearch(es_params)

    def get_metric_metadata(self, metric_name, tenant_id):
        """
        Get document from index metric_metadata for a given metric name and tenant id
        """

        document_id = self.get_document_id(tenant_id=tenant_id, metric_name=metric_name)
        try:
            return self.es.get(index='metric_metadata', doc_type='metrics', id=document_id, routing=tenant_id)
        except NotFoundError as e:
            return e.info

    def get_enums_data(self, metric_name, tenant_id):
        """
        Get document from index enums for a given metric name and tenant id
        """

        document_id = self.get_document_id(tenant_id=tenant_id, metric_name=metric_name)

        try:
            return self.es.get(index='enums', doc_type='metrics', id=document_id, routing=tenant_id)
        except NotFoundError as e:
            return e.info

    def delete_metric_metadata(self, metric_name, tenant_id):
        """
        Delete document from index metric_metadata for metric_metadata dictionary(obtained from get_metric_metadata
        call) and tenant id
        """

        document_id = self.get_document_id(tenant_id=tenant_id, metric_name=metric_name)
        self.es.delete(index='metric_metadata', doc_type='metrics', id=document_id, routing=tenant_id)
        print 'Deleted from index metric_metadata for _id: [%s] routing: [%s]' % (document_id, tenant_id)

    def delete_enums_data(self, metric_name, tenant_id):
        """
        Delete document from index enums for enums dictionary(obtained from get_enums_data
        call) and tenant id
        """
        document_id = self.get_document_id(tenant_id=tenant_id, metric_name=metric_name)
        self.es.delete(index='enums', doc_type='metrics', id=document_id, routing=tenant_id)
        print 'Deleted from index enums for _id: [%s] routing: [%s]' % (document_id, tenant_id)

    def get_document_id(self, tenant_id, metric_name):
        """
        Construct _id of elastic search from tenant id and metric name
        """
        return tenant_id + ':' + metric_name
