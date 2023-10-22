from .views import ExceptionsView, ReconStatsView, ReconcileView, ReversalsView, SettlementView, UploadedFilesViewset, sabsreconcile_csv_filesView
from rest_framework.routers import DefaultRouter
from django.urls import path,include

router = DefaultRouter()
router.register("files",UploadedFilesViewset,basename="files")

urlpatterns = [
    path("files/",include(router.urls)),
    path('reconcile/', ReconcileView.as_view(), name='reconcile'),
    path('reconstats/', ReconStatsView.as_view(), name='reconstats'),
    path('reversals/', ReversalsView.as_view(), name='reversals'),  # Add this line
    path('exceptions/', ExceptionsView.as_view(), name='exceptions'),
    path('settlementcsv_files/', SettlementView.as_view(), name='settlement-csv-files'),
    path('sabsreconcile_csv_file/', sabsreconcile_csv_filesView.as_view(), name='ssabsreconcile_csv_file'),

]