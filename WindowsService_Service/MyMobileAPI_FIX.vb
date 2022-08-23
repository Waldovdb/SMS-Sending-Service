Public Class MyMobileAPI_FIX
    Inherits MyMobileAPI.API

    Protected Overrides Function GetWebRequest(ByVal uri As Uri) As System.Net.WebRequest
        System.Net.ServicePointManager.Expect100Continue = False

        Dim webRequest As System.Net.HttpWebRequest = MyBase.GetWebRequest(uri)

        webRequest.KeepAlive = False

        Return webRequest
    End Function
End Class
