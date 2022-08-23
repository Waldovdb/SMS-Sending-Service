Imports System.Data
Imports System.Data.SqlClient
Imports System.Configuration
Imports System.IO.Compression

Public Class ProcessReporting
    Public IsRunning As Boolean = True
    Public Users As Users

    Public Sub Start()
        Dim ProcessReportingRun_Thread As New Threading.Thread(AddressOf ProcessReportingRun)
        ProcessReportingRun_Thread.Start()
    End Sub

    Private Sub ProcessReportingRun()
        Do While IsRunning = True
            Try
                Dim UserIDS As String = Users.GetUserIDS
                If Not UserIDS.Equals("") Then
                    For Each UserID As String In UserIDS.Split(",")
                        ProcessReports(UserID)
                    Next
                End If
            Catch ex As Exception
                CatchError("ProcessReportingRun", ex)
            Finally
                Threading.Thread.Sleep(1000)
            End Try
        Loop
    End Sub

    Private Sub ProcessReports(ByVal UserID As Integer)
        Try
            Dim TempUser As User = Users.GetUser(UserID)

            'DR's
            If TempUser.GetDRInterval > 0 Then
                If DateDiff(DateInterval.Minute, TempUser.LastProcessDR, Date.Now) >= TempUser.GetDRInterval Then
                    Users.UpdateLastCheckDR(UserID, Date.Now)
                    ProcessDR(UserID, TempUser)
                End If
            End If

            'Replies
            If TempUser.GetRepliesInterval > 0 Then
                If DateDiff(DateInterval.Minute, TempUser.LastProcessReplies, Date.Now) >= TempUser.GetRepliesInterval Then
                    Users.UpdateLastCheckReply(UserID, Date.Now)
                    ProcessReplies(UserID, TempUser)
                End If
            End If

            'Shortcodes
            If TempUser.GetSCInterval > 0 Then
                If DateDiff(DateInterval.Minute, TempUser.LastProcessSC, Date.Now) >= TempUser.GetSCInterval Then
                    Users.UpdateLastCheckSC(UserID, Date.Now)
                    ProcessShortcodes(UserID, TempUser)
                End If
            End If
        Catch ex As Exception
            CatchError("ProcessReports", ex)
        End Try
    End Sub

#Region " DR's "
    Public Sub ProcessDR(ByVal UserID As Integer, ByVal User As User)
        Try
            '-------------- Build input DS START --------------------
            Dim DS As New DataSet("sent")
            Dim DT As New DataTable("settings")
            DT.Columns.Add("id")
            DT.Columns.Add("cols_returned")
            DT.Columns.Add("date_format")

            Dim MainDR As DataRow = DT.NewRow
            MainDR.Item("id") = User.MaxDRID
            MainDR.Item("cols_returned") = "customerid,status,statusdate"
            MainDR.Item("date_format") = "dd/MMM/yyyy HH:mm:ss"
            DT.Rows.Add(MainDR)

            DS.Tables.Add(DT)
            '-------------- Build input DS END --------------------

            Dim DRDS As New DataSet
            Using API As New MyMobileAPI_FIX
                'Proxy Settings - if your using a Proxy/Firewall
                If Not ConfigurationManager.AppSettings.Get("ProxyServerIP").Equals("") Then
                    'Setup proxy IP and Port
                    Dim proxyObject As New System.Net.WebProxy(ConfigurationManager.AppSettings.Get("ProxyServerIP"), CInt(ConfigurationManager.AppSettings.Get("ProxyServerPort")))

                    'Check if part of domain
                    If ConfigurationManager.AppSettings.Get("ProxyServerDomain").Equals("") Then
                        'Proxy is not part of domain / check for username/password
                        If Not ConfigurationManager.AppSettings.Get("ProxyServerUsername").Equals("") Then
                            proxyObject.Credentials = New Net.NetworkCredential(ConfigurationManager.AppSettings.Get("ProxyServerUsername"), ConfigurationManager.AppSettings.Get("ProxyServerPassword"))
                        End If
                    Else
                        'Proxy is part of a domain / require domain,un,pw
                        proxyObject.Credentials = New Net.NetworkCredential(ConfigurationManager.AppSettings.Get("ProxyServerUsername"), ConfigurationManager.AppSettings.Get("ProxyServerPassword"), ConfigurationManager.AppSettings.Get("ProxyServerDomain"))
                    End If
                    API.Proxy = proxyObject
                End If

                DRDS.ReadXml(New IO.MemoryStream(Unzip(API.Sent_DS_ZIP(User.Username, User.Password, DS))))
            End Using

            'Check if complete call failed
            If CBool(DRDS.Tables("call_result").Rows(0).Item("result")) = False Then
                Users.RemoveUser(UserID)
            Else
                If DRDS.Tables.Contains("data") = True Then
                    For Each DRow As DataRow In DRDS.Tables("data").Rows
                        'Check if numeric
                        If IsNumeric(DRow.Item("customerid")) = True Then
                            If CLng(DRow.Item("changeid")) > User.MaxDRID Then
                                Users.UpdateMaxDRID(UserID, DRow.Item("changeid"))
                                DRItem(UserID, DRow)
                            End If
                        End If
                    Next

                    'Check to see if there is 100 records returned
                    If DRDS.Tables("data").Rows.Count >= 100 Then
                        Users.UpdateLastCheckDR(UserID, Date.Now.AddMinutes(-User.GetDRInterval)) 'Set it back to get the next 100 instantly
                    End If
                End If
            End If
        Catch ex As Exception
            CatchError("ProcessDR", ex)
        End Try
    End Sub

    Public Sub DRItem(ByVal UserID As Integer, ByVal DR As DataRow)
        '<data>
        '  <changeid>486</changeid>
        '  <customerid />
        '  <status>DELIVRD</status>
        '  <statusdate>10/mar/2009 15:34:12</statusdate>
        '</data>

        Try
            Using conn As New SqlConnection(ConfigurationManager.AppSettings.Get("ConnString"))
                Using SQLC As New SqlCommand("[Sent_Update]", conn)
                    SQLC.CommandType = CommandType.StoredProcedure
                    SQLC.Parameters.AddWithValue("@UserID", UserID)
                    SQLC.Parameters.AddWithValue("@ChangeID", DR.Item("changeid"))
                    SQLC.Parameters.AddWithValue("@ID", DR.Item("customerid"))
                    SQLC.Parameters.AddWithValue("@Status", DR.Item("status"))
                    SQLC.Parameters.AddWithValue("@StatusDatetime", DR.Item("statusdate"))
                    conn.Open()
                    SQLC.ExecuteNonQuery()
                End Using
            End Using
        Catch ex As Exception
            CatchError("DRItem", ex)
        End Try
    End Sub
#End Region

#Region " Replies "
    Public Sub ProcessReplies(ByVal UserID As Integer, ByVal User As User)
        Try
            '-------------- Build input DS START --------------------
            Dim DS As New DataSet("reply")
            Dim DT As New DataTable("settings")
            DT.Columns.Add("id")
            DT.Columns.Add("cols_returned")
            DT.Columns.Add("date_format")

            Dim MainDR As DataRow = DT.NewRow
            MainDR.Item("id") = User.MaxRepliesID
            MainDR.Item("cols_returned") = "numfrom,receiveddata,received,sentcustomerid,optout"
            MainDR.Item("date_format") = "dd/MMM/yyyy HH:mm:ss"
            DT.Rows.Add(MainDR)

            DS.Tables.Add(DT)
            '-------------- Build input DS END --------------------

            Dim DRDS As New DataSet
            Using API As New MyMobileAPI_FIX
                'Proxy Settings - if your using a Proxy/Firewall
                If Not ConfigurationManager.AppSettings.Get("ProxyServerIP").Equals("") Then
                    'Setup proxy IP and Port
                    Dim proxyObject As New System.Net.WebProxy(ConfigurationManager.AppSettings.Get("ProxyServerIP"), CInt(ConfigurationManager.AppSettings.Get("ProxyServerPort")))

                    'Check if part of domain
                    If ConfigurationManager.AppSettings.Get("ProxyServerDomain").Equals("") Then
                        'Proxy is not part of domain / check for username/password
                        If Not ConfigurationManager.AppSettings.Get("ProxyServerUsername").Equals("") Then
                            proxyObject.Credentials = New Net.NetworkCredential(ConfigurationManager.AppSettings.Get("ProxyServerUsername"), ConfigurationManager.AppSettings.Get("ProxyServerPassword"))
                        End If
                    Else
                        'Proxy is part of a domain / require domain,un,pw
                        proxyObject.Credentials = New Net.NetworkCredential(ConfigurationManager.AppSettings.Get("ProxyServerUsername"), ConfigurationManager.AppSettings.Get("ProxyServerPassword"), ConfigurationManager.AppSettings.Get("ProxyServerDomain"))
                    End If
                    API.Proxy = proxyObject
                End If

                DRDS.ReadXml(New IO.MemoryStream(Unzip(API.Reply_DS_ZIP(User.Username, User.Password, DS))))
            End Using

            'Check if complete call failed
            If CBool(DRDS.Tables("call_result").Rows(0).Item("result")) = False Then
                Users.RemoveUser(UserID)
            Else
                If DRDS.Tables.Contains("data") = True Then
                    For Each DRow As DataRow In DRDS.Tables("data").Rows
                        'Check if numeric
                        If IsNumeric(DRow.Item("sentcustomerid")) = True Then
                            If CLng(DRow.Item("replyid")) > User.MaxRepliesID Then
                                Users.UpdateMaxRepliesID(UserID, DRow.Item("replyid"))
                                ReplyItem(UserID, DRow)
                            End If
                        End If
                    Next

                    'Check to see if there is 100 records returned
                    If DRDS.Tables("data").Rows.Count >= 100 Then
                        Users.UpdateLastCheckReply(UserID, Date.Now.AddMinutes(-User.GetRepliesInterval)) 'Set it back to get the next 100 instantly
                    End If
                End If
            End If
        Catch ex As Exception
            CatchError("ProcessReplies", ex)
        End Try
    End Sub

    Public Sub ReplyItem(ByVal UserID As Integer, ByVal DR As DataRow)
        '<data>
        '  <replyid>3020746</replyid>
        '  <numfrom>27832297941</numfrom>
        '  <receiveddata>6</receiveddata>
        '  <sentcustomerid>UnqiueValue1</sentcustomerid>
        '  <received>20090304174418</received>
        '  <optout>False</optout>
        '</data>

        Try
            Using conn As New SqlConnection(ConfigurationManager.AppSettings.Get("ConnString"))
                Using SQLC As New SqlCommand("[Reply_Update]", conn)
                    SQLC.CommandType = CommandType.StoredProcedure
                    SQLC.Parameters.AddWithValue("@UserID", UserID)
                    SQLC.Parameters.AddWithValue("@ReplyID", DR.Item("replyid"))
                    SQLC.Parameters.AddWithValue("@ID", DR.Item("sentcustomerid"))
                    SQLC.Parameters.AddWithValue("@ReceivedData", DR.Item("receiveddata"))
                    SQLC.Parameters.AddWithValue("@ReceivedDatetime", DR.Item("received"))
                    SQLC.Parameters.AddWithValue("@OptOut", DR.Item("optout"))
                    conn.Open()
                    SQLC.ExecuteNonQuery()
                End Using
            End Using
        Catch ex As Exception
            CatchError("ReplyItem", ex)
        End Try
    End Sub
#End Region

#Region " Shortcodes "
    Public Sub ProcessShortcodes(ByVal UserID As Integer, ByVal User As User)
        Try
            '-------------- Build input DS START --------------------
            Dim DS As New DataSet("options")
            Dim DT As New DataTable("settings")
            DT.Columns.Add("id")
            DT.Columns.Add("date_format")

            Dim MainDR As DataRow = DT.NewRow
            MainDR.Item("id") = User.MaxSCID
            MainDR.Item("date_format") = "dd/MMM/yyyy HH:mm:ss"
            DT.Rows.Add(MainDR)

            DS.Tables.Add(DT)
            '-------------- Build input DS END --------------------

            Dim DRDS As New DataSet
            Using API As New MyMobileAPI_FIX
                'Proxy Settings - if your using a Proxy/Firewall
                If Not ConfigurationManager.AppSettings.Get("ProxyServerIP").Equals("") Then
                    'Setup proxy IP and Port
                    Dim proxyObject As New System.Net.WebProxy(ConfigurationManager.AppSettings.Get("ProxyServerIP"), CInt(ConfigurationManager.AppSettings.Get("ProxyServerPort")))

                    'Check if part of domain
                    If ConfigurationManager.AppSettings.Get("ProxyServerDomain").Equals("") Then
                        'Proxy is not part of domain / check for username/password
                        If Not ConfigurationManager.AppSettings.Get("ProxyServerUsername").Equals("") Then
                            proxyObject.Credentials = New Net.NetworkCredential(ConfigurationManager.AppSettings.Get("ProxyServerUsername"), ConfigurationManager.AppSettings.Get("ProxyServerPassword"))
                        End If
                    Else
                        'Proxy is part of a domain / require domain,un,pw
                        proxyObject.Credentials = New Net.NetworkCredential(ConfigurationManager.AppSettings.Get("ProxyServerUsername"), ConfigurationManager.AppSettings.Get("ProxyServerPassword"), ConfigurationManager.AppSettings.Get("ProxyServerDomain"))
                    End If
                    API.Proxy = proxyObject
                End If

                DRDS.ReadXml(New IO.MemoryStream(Unzip(API.ShortCode_Get_DS_ZIP(User.Username, User.Password, DS))))
            End Using

            'Check if complete call failed
            If CBool(DRDS.Tables("call_result").Rows(0).Item("result")) = False Then
                Users.RemoveUser(UserID)
            Else
                If DRDS.Tables.Contains("data") = True Then
                    For Each DRow As DataRow In DRDS.Tables("data").Rows
                        'Check if numeric
                        If IsNumeric(DRow.Item("changeid")) = True Then
                            If CLng(DRow.Item("changeid")) > User.MaxSCID Then
                                Users.UpdateMaxShortcodeID(UserID, DRow.Item("changeid"))
                                ShortcodeItem(UserID, DRow)
                            End If
                        End If
                    Next

                    'Check to see if there is 100 records returned
                    If DRDS.Tables("data").Rows.Count >= 100 Then
                        Users.UpdateLastCheckSC(UserID, Date.Now.AddMinutes(-User.GetSCInterval)) 'Set it back to get the next 100 instantly
                    End If
                End If
            End If
        Catch ex As Exception
            CatchError("ProcessReplies", ex)
        End Try
    End Sub

    Public Sub ShortcodeItem(ByVal UserID As Integer, ByVal DR As DataRow)
        '<data>
        '  <changeid>220910</changeid>
        '  <shortcode>34458</shortcode>
        '  <keyword>james</keyword>
        '  <phonenumber>27832297941</phonenumber>
        '  <message>James</message>
        '  <received>20081023231030</received>
        '</data>

        Try
            Using conn As New SqlConnection(ConfigurationManager.AppSettings.Get("ConnString"))
                Using SQLC As New SqlCommand("[Shortcode_Update]", conn)
                    SQLC.CommandType = CommandType.StoredProcedure
                    SQLC.Parameters.AddWithValue("@UserID", UserID)
                    SQLC.Parameters.AddWithValue("@ID", DR.Item("changeid"))
                    SQLC.Parameters.AddWithValue("@Shortcode", DR.Item("shortcode"))
                    SQLC.Parameters.AddWithValue("@NumFrom", DR.Item("phonenumber"))
                    SQLC.Parameters.AddWithValue("@Keyword", DR.Item("keyword"))
                    SQLC.Parameters.AddWithValue("@Data", DR.Item("message"))
                    SQLC.Parameters.AddWithValue("@Received", DR.Item("received"))
                    conn.Open()
                    SQLC.ExecuteNonQuery()
                End Using
            End Using
        Catch ex As Exception
            CatchError("ShortcodeItem", ex)
        End Try
    End Sub
#End Region

#Region " Compression "
    Public Function Unzip(ByVal CompressedByte() As Byte) As Byte()
        Try
            Dim objMemStream As New IO.MemoryStream(CompressedByte)
            Dim objGZipStream As New GZipStream(objMemStream, CompressionMode.Decompress)

            Dim sizeBytes(3) As Byte
            objMemStream.Position = objMemStream.Length - 5
            objMemStream.Read(sizeBytes, 0, 4)

            Dim iOutputSize As Integer = BitConverter.ToInt32(sizeBytes, 0)

            objMemStream.Position = 0

            Dim decompressedBytes(iOutputSize - 1) As Byte

            objGZipStream.Read(decompressedBytes, 0, iOutputSize)

            objGZipStream.Dispose()
            objMemStream.Dispose()

            Return decompressedBytes
        Catch ex As Exception
            Return Nothing
        End Try
    End Function
#End Region

#Region " Logging "
    Public Sub CatchError(ByVal FunctionName As String, ByVal ExError As Exception)
        Try
            IO.Directory.CreateDirectory("logging")
            Using STRW As New IO.StreamWriter("logging\Reporting_" & Date.Now.ToString("yyyyMMddhh") & ".txt", True)
                STRW.WriteLine(Date.Now.ToString("dd/MMM/yyyy HH:mm:ss"))
                STRW.WriteLine("Function : " & FunctionName)
                STRW.WriteLine("Error : " & ExError.Message)
                STRW.WriteLine("-------------------------------------------------------------------------------------")
            End Using
        Catch ex As Exception

        End Try
    End Sub
#End Region

End Class
