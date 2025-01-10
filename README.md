# AirflowUsefulStuff
This repo contains the codes , implementations , logics and all other stuff that i had implemented during working in different projects. Everything here is related to Airflow and its ecosystem.

Index With summary.
1) MyCustomCallbacksForKubernetes.py - If you want to execute your custom code before your Kubernetes pod operator execution or after the Pod completes or on creation before execution or in case of failure , then                                           callbacks are your bestfriends. KubernetesPodOperator provides a callback mechanism with different states of the Pod itself. These callbacks will be called automatically based                                         on which state the pod is in currently. This file provides a working callback example tested with airflow 2.10 version. The official documentation does not provide a clear cut                                         example, has mistakes and need to do a hit and try. You can refer this for quick source of Callback implementation.
2) DagRunUtilities.py - Contains utility functions which can be directly called to get specific DagRun objects and related data from the Metadata DB. This uses more SQLAlchemy approach than the convient ORM based                            approach.

