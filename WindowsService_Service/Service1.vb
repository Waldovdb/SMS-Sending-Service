Public Class Service1

    Dim Users As New Users
    Dim ProcessQueue As New ProcessQueue
    Dim ProcessReporting As New ProcessReporting

    Protected Overrides Sub OnStart(ByVal args() As String)
        Users.Start()

        ProcessQueue.Users = Users
        ProcessQueue.Start()

        ProcessReporting.Users = Users
        ProcessReporting.Start()
    End Sub

    Protected Overrides Sub OnStop()
        Users.IsRunning = False
        ProcessQueue.IsRunning = False
        ProcessReporting.IsRunning = False
    End Sub

End Class
