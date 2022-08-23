Imports System.Data
Imports System.Data.SqlClient
Imports System.Configuration

Public Class Users

    Public IsRunning As Boolean = True
    Private UserHT As New Hashtable

    Public Sub Start()
        Dim UpdateUsers_Thread As New Threading.Thread(AddressOf UpdateUsers)
        UpdateUsers_Thread.Start()

        Dim UserCheckCredits_Thread As New Threading.Thread(AddressOf UserCheckCredits)
        UserCheckCredits_Thread.Start()
    End Sub

#Region " Get User from DB "
    Private Sub UpdateUsers()
        Do While IsRunning = True
            Try
                Dim UserDT As New DataTable("SMSData")
                Using conn As New SqlConnection(ConfigurationManager.AppSettings.Get("ConnString"))
                    Using SQLC As New SqlCommand("[Users_List]", conn)
                        SQLC.CommandType = CommandType.StoredProcedure
                        conn.Open()
                        Using DR As SqlDataReader = SQLC.ExecuteReader
                            UserDT.Load(DR)
                        End Using
                    End Using
                End Using

                SyncLock UserHT.SyncRoot
                    UserHT.Clear()
                    For Each UserDR As DataRow In UserDT.Rows
                        UpdateUsersAdd(UserDR)
                    Next
                End SyncLock
            Catch ex As Exception
                CatchError("UpdateUsers", ex)
            Finally
                Threading.Thread.Sleep(30000) 'Sleep for 30 seconds, then refresh detail from DB
            End Try
        Loop
    End Sub

    Public Sub UpdateUsersAdd(ByVal DR As DataRow)
        Try
            Dim NewUser As New User
            NewUser.UserID = DR.Item("UserID")
            NewUser.Username = DR.Item("Username")
            NewUser.Password = DR.Item("Password")
            NewUser.Credits = DR.Item("Credits")
            NewUser.Active = DR.Item("Active")
            NewUser.SendStartHour = DR.Item("SendStartHour")
            NewUser.SendEndHour = DR.Item("SendEndHour")

            NewUser.GetRepliesInterval = DR.Item("GetRepliesInterval")
            NewUser.GetDRInterval = DR.Item("GetDRInterval")
            NewUser.GetSCInterval = DR.Item("GetSCInterval")
            NewUser.ProcessQueueInterval = DR.Item("ProcessQueueInterval")

            NewUser.MaxRepliesID = DR.Item("MaxRepliesID")
            NewUser.MaxDRID = DR.Item("MaxDRID")
            NewUser.MaxSCID = DR.Item("MaxSCID")

            If Not IsDBNull(DR.Item("LastProcessReplies")) Then
                NewUser.LastProcessReplies = DR.Item("LastProcessReplies")
            End If
            If Not IsDBNull(DR.Item("LastProcessDR")) Then
                NewUser.LastProcessDR = DR.Item("LastProcessDR")
            End If
            If Not IsDBNull(DR.Item("LastProcessSC")) Then
                NewUser.LastProcessSC = DR.Item("LastProcessSC")
            End If
            If Not IsDBNull(DR.Item("LastProcessQueue")) Then
                NewUser.LastProcessQueue = DR.Item("LastProcessQueue")
            End If

            If NewUser.Active = True Then
                UserHT.Add(NewUser.UserID, NewUser)
            End If
        Catch ex As Exception
            CatchError("UpdateUsersAdd", ex)
        End Try
    End Sub
#End Region

#Region " Check User for credits etc "
    Private Sub UserCheckCredits()
        Do While IsRunning = True
            Try
                Dim USERIDQUEUE As New Queue
                SyncLock UserHT.SyncRoot
                    For Each DI As DictionaryEntry In UserHT
                        Dim TempUser As User = CType(DI.Value, User)
                        If TempUser.Credits <= 0 Then
                            USERIDQUEUE.Enqueue(TempUser)
                        End If
                    Next
                End SyncLock

                Do While USERIDQUEUE.Count > 0
                    Dim TempUser As User = USERIDQUEUE.Dequeue
                    Try
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

                            Dim TempDS As DataSet = API.Credits_DS(TempUser.Username, TempUser.Password)
                            If CBool(TempDS.Tables("call_result").Rows(0).Item("result")) = True Then
                                UpdateUserCredits(TempUser.UserID, TempDS.Tables("data").Rows(0).Item("credits"))
                            End If
                        End Using
                    Catch ex As Exception
                        CatchError("UserCheckCredits-inner", ex)
                    End Try
                Loop
            Catch ex As Exception
                CatchError("UserCheckCredits", ex)
            Finally
                Threading.Thread.Sleep(60000) 'Check users with 0 credits every 60 seconds
            End Try
        Loop
    End Sub

    Public Sub UpdateUserCredits(ByVal UserID As Integer, ByVal Credits As Integer)
        Using conn As New SqlConnection(ConfigurationManager.AppSettings.Get("ConnString"))
            Using SQLC As New SqlCommand("[Users_Credits_Update]", conn)
                SQLC.CommandType = CommandType.StoredProcedure
                SQLC.Parameters.AddWithValue("@UserID", UserID)
                SQLC.Parameters.AddWithValue("@Credits", Credits)
                conn.Open()
                SQLC.ExecuteNonQuery()
            End Using
        End Using
    End Sub

#End Region

    Public Function GetUserIDS() As String
        Dim USERIDS As String = ""
        SyncLock UserHT.SyncRoot
            For Each DI As DictionaryEntry In UserHT
                If USERIDS.Equals("") Then
                    USERIDS = DI.Key
                Else
                    USERIDS += "," & DI.Key
                End If
            Next
        End SyncLock

        Return USERIDS
    End Function

    Public Function GetUserIDStoSend() As String
        Dim USERIDS As String = ""
        SyncLock UserHT.SyncRoot
            For Each DI As DictionaryEntry In UserHT
                Dim TempUser As User = CType(DI.Value, User)
                'Check if user has credits and interval > 0
                If TempUser.Credits > 0 And TempUser.ProcessQueueInterval > 0 Then
                    'Check if the account can send between the time stipulated
                    If TempUser.SendStartHour <= Date.Now.Hour And TempUser.SendEndHour >= Date.Now.Hour Then
                        'check if we have not already processed the account
                        If DateDiff(DateInterval.Second, TempUser.LastProcessQueue, Date.Now) >= TempUser.ProcessQueueInterval Then
                            If USERIDS.Equals("") Then
                                USERIDS = DI.Key
                            Else
                                USERIDS += "," & DI.Key
                            End If
                        End If
                    End If
                End If
            Next
        End SyncLock

        Return USERIDS
    End Function

    Public Function GetUser(ByVal UserID As Integer) As User
        Dim TempUSer As New User
        SyncLock UserHT.SyncRoot
            If UserHT.Contains(UserID) Then
                TempUSer = UserHT.Item(UserID)
            Else
                TempUSer = Nothing
            End If
        End SyncLock
        Return TempUSer
    End Function

    Public Sub RemoveUser(ByVal UserID As Integer)
        UpdateUserCredits(UserID, 0)
        UserHT.Remove(UserID)
    End Sub

    Public Sub UpdateLastCheckQueue(ByVal UserID As Integer, ByVal DT As DateTime)
        SyncLock UserHT.SyncRoot
            If UserHT.Contains(UserID) Then
                Dim TempUSer As User = UserHT.Item(UserID)
                TempUSer.LastProcessQueue = DT
            End If
        End SyncLock

        Using conn As New SqlConnection(ConfigurationManager.AppSettings.Get("ConnString"))
            Using SQLC As New SqlCommand("[Queue_LastProcess_Update]", conn)
                SQLC.CommandType = CommandType.StoredProcedure
                SQLC.Parameters.AddWithValue("@UserID", UserID)
                SQLC.Parameters.AddWithValue("@DT", DT)
                conn.Open()
                SQLC.ExecuteNonQuery()
            End Using
        End Using
    End Sub

    Public Sub UpdateLastCheckDR(ByVal UserID As Integer, ByVal DT As DateTime)
        SyncLock UserHT.SyncRoot
            If UserHT.Contains(UserID) Then
                Dim TempUSer As User = UserHT.Item(UserID)
                TempUSer.LastProcessDR = DT
            End If
        End SyncLock

        Using conn As New SqlConnection(ConfigurationManager.AppSettings.Get("ConnString"))
            Using SQLC As New SqlCommand("[Sent_UserLastCheck_Update]", conn)
                SQLC.CommandType = CommandType.StoredProcedure
                SQLC.Parameters.AddWithValue("@UserID", UserID)
                SQLC.Parameters.AddWithValue("@DT", DT)
                conn.Open()
                SQLC.ExecuteNonQuery()
            End Using
        End Using
    End Sub

    Public Sub UpdateLastCheckReply(ByVal UserID As Integer, ByVal DT As DateTime)
        SyncLock UserHT.SyncRoot
            If UserHT.Contains(UserID) Then
                Dim TempUSer As User = UserHT.Item(UserID)
                TempUSer.LastProcessReplies = DT
            End If
        End SyncLock

        Using conn As New SqlConnection(ConfigurationManager.AppSettings.Get("ConnString"))
            Using SQLC As New SqlCommand("[Reply_UserLastCheck_Update]", conn)
                SQLC.CommandType = CommandType.StoredProcedure
                SQLC.Parameters.AddWithValue("@UserID", UserID)
                SQLC.Parameters.AddWithValue("@DT", DT)
                conn.Open()
                SQLC.ExecuteNonQuery()
            End Using
        End Using
    End Sub

    Public Sub UpdateLastCheckSC(ByVal UserID As Integer, ByVal DT As DateTime)
        SyncLock UserHT.SyncRoot
            If UserHT.Contains(UserID) Then
                Dim TempUSer As User = UserHT.Item(UserID)
                TempUSer.LastProcessSC = DT
            End If
        End SyncLock

        Using conn As New SqlConnection(ConfigurationManager.AppSettings.Get("ConnString"))
            Using SQLC As New SqlCommand("[Shortcode_UserLastCheck_Update]", conn)
                SQLC.CommandType = CommandType.StoredProcedure
                SQLC.Parameters.AddWithValue("@UserID", UserID)
                SQLC.Parameters.AddWithValue("@DT", DT)
                conn.Open()
                SQLC.ExecuteNonQuery()
            End Using
        End Using
    End Sub

    Public Sub UpdateMaxDRID(ByVal UserID As Integer, ByVal ID As Long)
        SyncLock UserHT.SyncRoot
            If UserHT.Contains(UserID) Then
                Dim TempUSer As User = UserHT.Item(UserID)
                TempUSer.MaxDRID = ID
            End If
        End SyncLock
    End Sub

    Public Sub UpdateMaxRepliesID(ByVal UserID As Integer, ByVal ID As Long)
        SyncLock UserHT.SyncRoot
            If UserHT.Contains(UserID) Then
                Dim TempUSer As User = UserHT.Item(UserID)
                TempUSer.MaxRepliesID = ID
            End If
        End SyncLock
    End Sub

    Public Sub UpdateMaxShortcodeID(ByVal UserID As Integer, ByVal ID As Long)
        SyncLock UserHT.SyncRoot
            If UserHT.Contains(UserID) Then
                Dim TempUSer As User = UserHT.Item(UserID)
                TempUSer.MaxSCID = ID
            End If
        End SyncLock
    End Sub

#Region " Logging "
    Public Sub CatchError(ByVal FunctionName As String, ByVal ExError As Exception)
        Try
            IO.Directory.CreateDirectory("logging")
            Using STRW As New IO.StreamWriter("logging\Users_" & Date.Now.ToString("yyyyMMddhh") & ".txt", True)
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

Public Class User
    Public UserID As Integer = 0
    Public Username As String = ""
    Public Password As String = ""
    Public Credits As Integer = 0
    Public Active As Boolean = False
    Public SendStartHour As Integer = 0
    Public SendEndHour As Integer = 23
    Public GetRepliesInterval As Integer = 0 'minutes
    Public GetDRInterval As Integer = 0 'minutes
    Public GetSCInterval As Integer = 0 'minutes
    Public ProcessQueueInterval = 30 'seconds
    Public MaxRepliesID As Long = Long.MaxValue
    Public MaxDRID As Long = Long.MaxValue
    Public MaxSCID As Long = Long.MaxValue
    Public LastProcessReplies As DateTime = Date.Now.AddDays(-1)
    Public LastProcessDR As DateTime = Date.Now.AddDays(-1)
    Public LastProcessSC As DateTime = Date.Now.AddDays(-1)
    Public LastProcessQueue As DateTime = Date.Now.AddDays(-1)
End Class
