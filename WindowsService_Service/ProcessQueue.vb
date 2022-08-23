Imports System.Data
Imports System.Data.SqlClient
Imports System.Configuration
Imports System.IO.Compression

Public Class ProcessQueue

    Public IsRunning As Boolean = True
    Public Users As Users

    Public Sub Start()
        Dim ProcessQueueRun_Thread As New Threading.Thread(AddressOf ProcessQueueRun)
        ProcessQueueRun_Thread.Start()

        Dim ProcessRetryCount_Thread As New Threading.Thread(AddressOf ProcessRetryCount)
        ProcessRetryCount_Thread.Start()
    End Sub

#Region " Process QUEUE "
    Private Sub ProcessQueueRun()
        Do While IsRunning = True
            Try
                Dim UserIDS As String = Users.GetUserIDStoSend
                If Not UserIDS.Equals("") Then
                    Dim QueueDT As New DataTable("SMSData")
                    Using conn As New SqlConnection(ConfigurationManager.AppSettings.Get("ConnString"))
                        Using SQLC As New SqlCommand("[Queue_List]", conn)
                            SQLC.CommandType = CommandType.StoredProcedure
                            SQLC.Parameters.AddWithValue("@UserIDs", UserIDS)
                            conn.Open()
                            Using DR As SqlDataReader = SQLC.ExecuteReader
                                QueueDT.Load(DR)
                            End Using
                        End Using
                    End Using

                    For Each UserID As String In UserIDS.Split(",")
                        SendQueue(UserID, QueueDT.Copy)
                    Next
                End If
            Catch ex As Exception
                CatchError("ProcessQueueRun", ex)
            Finally
                Threading.Thread.Sleep(1000)
            End Try
        Loop
    End Sub

    Private Sub SendQueue(ByVal UserID As Integer, ByVal Data As DataTable)
        Try
            Dim TempUser As User = Users.GetUser(UserID)
            Dim Credits As Integer = TempUser.Credits

            For i As Integer = Data.Rows.Count - 1 To 0 Step -1
                If CInt(Data.Rows(i).Item("UserID")) <> UserID Then
                    Data.Rows.RemoveAt(i)
                End If
            Next

            If Data.Rows.Count = 0 Then
                'No data to send for this user
                Users.UpdateLastCheckQueue(UserID, Date.Now)
                Exit Sub
            Else
                'Set the time back so it will get the next set of data instantly
                Users.UpdateLastCheckQueue(UserID, Date.Now.AddSeconds(-TempUser.ProcessQueueInterval))
            End If

            Dim SendIDS As String = ""
            Dim DS As New DataSet("senddata")

            ' --------------------------------- Settings --------------------------------- 
            Dim DT_Settings As New DataTable("settings")
            DT_Settings.Columns.Add("return_credits") 'OPTIONAL - True/False
            DT_Settings.Columns.Add("return_entries_failed_status") 'OPTIONAL - True/False (default - false)
            DT_Settings.Columns.Add("default_date") 'REQUIRED - dd/MMM/yyyy
            DT_Settings.Columns.Add("default_time") 'REQUIRED - HH:mm

            Dim MainDR As DataRow = DT_Settings.NewRow
            MainDR.Item("return_credits") = "True"
            MainDR.Item("return_entries_failed_status") = "True"
            MainDR.Item("default_date") = Date.Now.AddYears(-1).ToString("dd/MMM/yyyy")
            MainDR.Item("default_time") = Date.Now.AddYears(-1).ToString("HH:mm")
            DT_Settings.Rows.Add(MainDR)
            DS.Tables.Add(DT_Settings)

            ' --------------------------------- Entries --------------------------------- 
            Dim DT_Entries As New DataTable("entries")
            DT_Entries.Columns.Add(New DataColumn("numto")) 'REQUIRED
            DT_Entries.Columns.Add(New DataColumn("customerid")) 'REQUIRED
            DT_Entries.Columns.Add(New DataColumn("senderid")) 'OPTIONAL (will assume default_senderid if not present)
            DT_Entries.Columns.Add(New DataColumn("data1")) 'OPTIONAL (will assume default_data1 if not present)
            DT_Entries.Columns.Add(New DataColumn("data2")) 'OPTIONAL (will assume default_data2 if not present)
            DT_Entries.Columns.Add(New DataColumn("flash")) 'OPTIONAL (will assume default_flash if not present)
            DT_Entries.Columns.Add(New DataColumn("type")) 'OPTIONAL (will assume default_type if not present)
            DT_Entries.Columns.Add(New DataColumn("costcentre")) 'OPTIONAL (will assume default_costcentre if not present)

            For Each SMSDR As DataRow In Data.Rows
                If SendIDS.Equals("") Then
                    SendIDS = SMSDR.Item("id")
                Else
                    SendIDS += "," & SMSDR.Item("id")
                End If

                Select Case SMSDR.Item("Type").ToString.ToUpper
                    Case "VCARD"
                        Dim DR_Entry As DataRow = DT_Entries.NewRow
                        DR_Entry.Item("senderid") = SMSDR.Item("senderid")
                        DR_Entry.Item("numto") = SMSDR.Item("numto")
                        DR_Entry.Item("data1") = SMSDR.Item("data1")
                        DR_Entry.Item("data2") = SMSDR.Item("data2")
                        DR_Entry.Item("flash") = "False"
                        DR_Entry.Item("type") = "VCARD"
                        DR_Entry.Item("customerid") = SMSDR.Item("ID")
                        DR_Entry.Item("costcentre") = SMSDR.Item("costcentre")
                        DT_Entries.Rows.Add(DR_Entry)
                        Credits -= 1
                    Case "WPUSH"
                        Dim DR_Entry As DataRow = DT_Entries.NewRow
                        DR_Entry.Item("senderid") = SMSDR.Item("senderid")
                        DR_Entry.Item("numto") = SMSDR.Item("numto")
                        DR_Entry.Item("data1") = SMSDR.Item("data1")
                        DR_Entry.Item("data2") = SMSDR.Item("data2")
                        DR_Entry.Item("flash") = "False"
                        DR_Entry.Item("type") = "WPUSH"
                        DR_Entry.Item("customerid") = SMSDR.Item("ID")
                        DR_Entry.Item("costcentre") = SMSDR.Item("costcentre")
                        DT_Entries.Rows.Add(DR_Entry)
                        Credits -= 1
                    Case Else
                        Dim DR_Entry As DataRow = DT_Entries.NewRow
                        DR_Entry.Item("senderid") = SMSDR.Item("senderid")
                        DR_Entry.Item("numto") = SMSDR.Item("numto")
                        DR_Entry.Item("data1") = SMSDR.Item("data1")
                        DR_Entry.Item("data2") = ""
                        DR_Entry.Item("flash") = SMSDR.Item("flash")
                        DR_Entry.Item("type") = "SMS"
                        DR_Entry.Item("customerid") = SMSDR.Item("ID")
                        DR_Entry.Item("costcentre") = SMSDR.Item("costcentre")
                        DT_Entries.Rows.Add(DR_Entry)
                        Credits -= 1
                End Select
                If Credits <= 0 Then
                    Exit For
                End If
            Next
            DS.Tables.Add(DT_Entries)

            'Update Retry count for the messages and LastPRocessed on user table
            Using conn As New SqlConnection(ConfigurationManager.AppSettings.Get("ConnString"))
                Using SQLC As New SqlCommand("[Queue_Processing]", conn)
                    SQLC.CommandType = CommandType.StoredProcedure
                    SQLC.Parameters.AddWithValue("@UserID", UserID)
                    SQLC.Parameters.AddWithValue("@IDS", SendIDS)
                    conn.Open()
                    SQLC.ExecuteNonQuery()
                End Using
            End Using

            Dim TempDS As New DataSet
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

                TempDS.ReadXml(New IO.MemoryStream(Unzip(API.Send_ZIP_ZIP(TempUser.Username, TempUser.Password, Zip(System.Text.Encoding.UTF8.GetBytes(DS.GetXml))))))
            End Using

            'Fail individual items
            If TempDS.Tables.Contains("entries_failed") Then
                For Each FailedDR As DataRow In TempDS.Tables("entries_failed").Rows
                    FailItem(FailedDR)
                Next
            End If

            'Check if complete call failed
            If CBool(TempDS.Tables("call_result").Rows(0).Item("result")) = False Then
                If TempDS.Tables.Contains("entries_failed") = False Then
                    Users.RemoveUser(UserID)
                End If
            Else
                Users.UpdateUserCredits(TempUser.UserID, TempDS.Tables("send_info").Rows(0).Item("credits"))

                For Each ID As String In SendIDS.Split(",")
                    SuccessItem(ID)
                Next
            End If
        Catch ex As Exception
            CatchError("SendQueue", ex)
        End Try
    End Sub

    Public Sub FailItem(ByVal FailedDR As DataRow)
        Try
            Using conn As New SqlConnection(ConfigurationManager.AppSettings.Get("ConnString"))
                Using SQLC As New SqlCommand("[Queue_Failed]", conn)
                    SQLC.CommandType = CommandType.StoredProcedure
                    SQLC.Parameters.AddWithValue("@ID", FailedDR.Item("customerid"))
                    SQLC.Parameters.AddWithValue("@Reason", FailedDR.Item("reason"))
                    conn.Open()
                    SQLC.ExecuteNonQuery()
                End Using
            End Using
        Catch ex As Exception
            CatchError("FailItem", ex)
        End Try
    End Sub

    Public Sub SuccessItem(ByVal ID As Long)
        Try
            Using conn As New SqlConnection(ConfigurationManager.AppSettings.Get("ConnString"))
                Using SQLC As New SqlCommand("[Queue_Success]", conn)
                    SQLC.CommandType = CommandType.StoredProcedure
                    SQLC.Parameters.AddWithValue("@ID", ID)
                    conn.Open()
                    SQLC.ExecuteNonQuery()
                End Using
            End Using
        Catch ex As Exception
            CatchError("SuccessItem", ex)
        End Try
    End Sub
#End Region

#Region " Moved RetryCount "
    Private Sub ProcessRetryCount()
        Do While IsRunning = True
            Try
                Using conn As New SqlConnection(ConfigurationManager.AppSettings.Get("ConnString"))
                    Using SQLC As New SqlCommand("[Queue_FailRetry]", conn)
                        SQLC.CommandType = CommandType.StoredProcedure
                        conn.Open()
                        SQLC.ExecuteNonQuery()
                    End Using
                End Using
            Catch ex As Exception
                CatchError("ProcessRetryCount", ex)
            Finally
                Threading.Thread.Sleep(120000) 'Check QUEUE for retrcount >= 3 every 2 minutes and move to SEnt table
            End Try
        Loop
    End Sub
#End Region

#Region " Compression "
    Public Function Zip(ByVal byteSource() As Byte) As Byte()
        Dim objMemStream As New IO.MemoryStream()

        Using objGZipStream As New GZipStream(objMemStream, CompressionMode.Compress, True)
            objGZipStream.Write(byteSource, 0, byteSource.Length)
        End Using

        objMemStream.Position = 0

        ' Write compressed memory stream into byte array
        Dim buffer(objMemStream.Length) As Byte
        objMemStream.Read(buffer, 0, buffer.Length)
        objMemStream.Dispose()

        Return buffer
    End Function

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
            Using STRW As New IO.StreamWriter("logging\Queue_" & Date.Now.ToString("yyyyMMddhh") & ".txt", True)
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
