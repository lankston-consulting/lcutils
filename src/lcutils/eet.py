import ee


class EeTools(object):
    """
    EE Helper forms a singleton object that imports the EE library and authenticates. Using this helper keeps
    authentications down to a single one per run, no matter how many times the EE library is loaded.
    NOTE: for this to work, don't authenticate EE anywhere else in the project
    """

    _instance = None

    def __new__(cls, *args, **kwargs):
        """
        Singleton catcher. Can optionally pass a kwarg or "use_service_account" with a value of
        {'account': service_account@domain, 'keyfile'=path_to_json_keyfile} to authenticate as a service account
        :param args:
        :param kwargs: use_service_account (optional)
        """
        if cls._instance is None:
            cls._instance = super(EeTools, cls).__new__(cls)

            if "use_service_account" in kwargs:
                ee_account = kwargs["use_service_account"]
                ee_credentials = ee.ServiceAccountCredentials(
                    ee_account["account"], ee_account["keyfile"]
                )
                ee.Initialize(ee_credentials)
            else:
                ee.Initialize()

        return cls._instance

    @staticmethod
    def list_assets(project, folder):
        """
        Pass a project and folder to list available assets. Note that depending on current authentication
        it will only return what you are authenticated to see.
        If any assets are matched, returns a list with items in the form
        {type:, name:, id:}
        See https://developers.google.com/earth-engine/reference/rest/v1alpha/projects.assets/listAssets
        :param project:
        :param folder:
        :return:
        """
        if folder == "":
            matched = ee.data.listAssets({"parent": f"{project}/"})
        else:
            matched = ee.data.listAssets({"parent": f"{project}/{folder}/"})
        assets = None
        if matched:
            assets = matched["assets"]
        return assets

    @staticmethod
    def copy_collection(
        source_project, source_collection, dest_project, dest_collection
    ):
        asset_list = EeTools.list_assets(source_project, source_collection)
        for elem in asset_list:
            source = elem["id"]
            short_name = (
                source[source.rfind("/") + 1 :].replace("_", "-").replace("rpms-", "")
            )
            print(short_name)

            dest = dest_project + "/" + dest_collection + "/" + short_name
            print(dest)

            ee.data.copyAsset(source, dest)
        return

    @staticmethod
    def delete_assets(project, asset):
        asset_list = EeTools.list_assets(project, asset)
        for elem in asset_list:
            source = elem["id"]
            print("Deleting", source)
            ee.data.deleteAsset(source)
