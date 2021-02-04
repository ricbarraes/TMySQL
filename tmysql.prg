#include 'mercury.ch'
#include "FileIO.ch"

#ifdef __PLATFORM__WINDOWS
   #include "c:\harbour19\include\hbdyn.ch"
#else
   #include "/harbour/include/hbdyn.ch"
#endif

#define HB_VERSION_BITWIDTH  17
#define NULL 0         

FUNCTION hb_SysLong()

return If( hb_OSIS64BIT(), HB_DYN_CTYPE_LLONG_UNSIGNED, HB_DYN_CTYPE_LONG_UNSIGNED )   

FUNCTION oBD()

   STATIC oBD
   IF oBD == NIL
      oBD:=TMySql():New()
   ENDIF

RETURN oBD


CLASS TMySql

   DATA cSGBD
   DATA cHost									
	DATA cUser									
 	DATA cSchema								
   DATA nPort
   DATA pLib
   DATA cLibName
   DATA pLib
   DATA hMySQL
   DATA cPsw
   
   DATA  hConnection 
   DATA  Tables   INIT {}
   DATA  nRetVal
 
   METHOD New( cSgbd, cHost, cUser, cPsw, cSchema, nPort ) 
   METHOD Query( cSQL ) 
   METHOD Escape(cParam)
   METHOD Exec(cSQL)
   METHOD JsonToTable(hJson)

 
 ENDCLASS
 
 //----------------------------------------------------------------------------//
 
 METHOD New(cSgbd, cHost, cUser, cPsw, cSchema, nPort) CLASS TMySql
 
   IF cSgbd # NIL .OR. .NOT. Empty(AP_GETENV( 'SGBD' ))      
      ::cSGBD    := IF(cSgbd # NIL,cSgbd,IF (Empty(AP_GETENV( 'SGBD' )),"MYSQL",AP_GETENV( 'SGBD' )))
      ::cHost		:= IF(cHost # NIL,cHost,AP_GETENV( 'HOST' ))
      ::cUser		:= IF(cUser # NIL,cUser,AP_GETENV( 'USER' ))   	
      ::cSchema	:= IF(cSchema # NIL,cSchema,AP_GETENV( 'SCHEMA' ))
      ::nPort		:= IF(nPort # NIL,nPort,3306)
      ::cPsw     := IF(cPsw # NIL, cPsw,NIL)

      IF .NOT. "Windows" $ OS()
         ::cLibName = If( hb_version( HB_VERSION_BITWIDTH ) == 64,;
                        IF(::cSGBD=="MYSQL","/usr/lib/x86_64-linux-gnu/libmysqlclient.so.21","/usr/lib/x86_64-linux-gnu/libmariadb.so.3"),; // libmysqlclient.so.20 for mariaDB
                        IF(::cSGBD=="MYSQL","/usr/lib/x86-linux-gnu/libmysqlclient.so.21","/usr/lib/x86-linux-gnu/libmariadbclient.so") )    
   
      ELSE
         ::cLibName = If( hb_version( HB_VERSION_BITWIDTH ) == 64,;
                           IF(::cSGBD=="MYSQL","c:/xampp/htdocs/libmysql64.dll","c:/xampp/htdocs/libmariadb64.dll"),;
                              IF(::cSGBD=="MYSQL","c:/xampp/htdocs/libmysql.dll","c:/xampp/htdocs/libmariadb.dll"))
      ENDIF
         
      ::pLib = hb_LibLoad( ::cLibName )
   ENDIF
    
 return Self

 
 //----------------------------------------------------------------------------//
 METHOD Exec( aSQL, aParam )

   LOCAL nArq, nCount, nCountCol

   if ! Empty( ::pLib )
      ::hMySQL = mysql_init(::pLib)        
         
      if ::hMySQL != 0
         ::hConnection = mysql_real_connect( ::pLib,::cHost, ::cUser, IF(::cPsw # NIL,::cPsw, AP_GETENV( 'PASSWORD' )), ::cSchema, ::nPort, ::hMySQL )
         
         if ::hConnection != ::hMySQL
            //? "Error on connection to server " + ::cHost,::hConnection , ::hMySQL
            RETURN NIL
         endif
      endif 

      FOR nCount:=1 TO Len(aSQL)
         IF aParam # NIL .AND. Len(aParam[nCount]) > 0        
            FOR nCountCol:=1 TO Len(aParam[nCount])
               mysql_real_escape_string_quote(::pLib,::hConnection,@aParam[nCount,nCountCol])
               aSQl[nCount]:=StrTran(aSQL[nCount],"PARAM"+StrZero(nCountCol,2),IF(Type(aParam[nCount,nCountCol]) # "C",IF(Type(aParam[nCount,nCountCol]) = "D", Dtoc(aParam[nCount,nCountCol]),hb_ValToStr(aParam[nCount,nCountCol])),aParam[nCount,nCountCol]))
            NEXT nCountCol
         ENDIF
         mysql_real_query( ::pLib,  ::hConnection, aSQL[nCount] )     
      NEXT nCount
      
      if mysql_error( ::hMySQL ) # NIL         
         nArq := FCreate("MySql.log")
         FWrite( nArq, mysql_error( ::hMySQL ) )
         FClose( nArq )
      endif 

      mysql_close(::pLib,::hConnection)
   else
      //? ::cLibName + " not available"     
      RETURN NIL
   endif   

 RETURN NIL

 METHOD Query(cSQL, aParam)
   
   LOCAL oTable,nCount

   if ! Empty( ::pLib )
      ::hMySQL = mysql_init(::pLib)        
      
      if ::hMySQL != 0
         ::hConnection = mysql_real_connect( ::pLib,::cHost, ::cUser,IF(::cPsw # NIL,::cPsw, AP_GETENV( 'PASSWORD' )), ::cSchema, ::nPort, ::hMySQL )
         
         if ::hConnection != ::hMySQL
            ? "Error on connection to server " + ::cHost,::cSchema,::cUser,::hConnection , ::hMySQL, ::cLibName, mysql_error(::pLib,::hMySQL)
            RETURN NIL
         endif
      endif 

      IF .NOT. Empty(aParam)
         FOR nCount:=1 TO Len(aParam) 
            mysql_real_escape_string_quote(::pLib,::hConnection,@aParam[nCount])
            cSQL:=StrTran(cSQL,"PARAM"+StrZero(nCount,2),IF(Type(aParam[nCount]) # "C",IF(Type(aParam[nCount]) = "D", Dtoc(aParam[nCount]),hb_ValToStr(aParam[nCount])),aParam[nCount]))
         NEXT nCount
      ENDIF

      ::nRetVal := mysql_real_query( ::pLib, ::hConnection, cSQL )     

      if ::nRetVal != 0
         oTable:=NIL
      else
         oTable = MySqlTable():New(Self)
      endif 

      mysql_close(::pLib,::hConnection)
   else
      ? ::cLibName + " not available"     
      RETURN NIL
   endif   
 RETURN oTable 

 METHOD JsonToTable(hJson)
   
   LOCAL oTable

   oTable = MySqlTable():New(Self,hJson)

 RETURN oTable


 METHOD Escape(cParam)
   
   LOCAL nTeste
   if ! Empty( ::pLib )
      ::hMySQL = mysql_init(::pLib)       
         
      if ::hMySQL != 0
         ::hConnection = mysql_real_connect( ::pLib,::cHost, ::cUser, IF(::cPsw # NIL,::cPsw, AP_GETENV( 'PASSWORD' )), ::cSchema, ::nPort, ::hMySQL )
         
         if ::hConnection != ::hMySQL
            //? "Error on connection to server " + ::cHost,::hConnection , ::hMySQL
            RETURN NIL
         endif
      endif 
      
      nTeste:=mysql_real_escape_string_quote(::pLib,::hConnection,@cParam)
      
      mysql_close(::pLib,::hConnection)
   else
      ? ::cLibName + " not available"     
   endif   
 

 RETURN cParam
 
//-----------------------------------------------------------------------------//
 
 CLASS OrmTable
 
    DATA   Orm
    DATA   aFields
 
    METHOD New(oOrm)
 
    METHOD Count()  VIRTUAL 
    METHOD FCount() VIRTUAL   
    METHOD FieldName( n ) VIRTUAL
    METHOD FieldGet( ncField ) VIRTUAL
    METHOD FieldPut( ncField, uValue ) VIRTUAL   
    METHOD FieldPos( cFieldName )
    METHOD FieldType( cnField ) VIRTUAL   
    METHOD First() VIRTUAL
    METHOD Last()  VIRTUAL         
    METHOD Next()  VIRTUAL  
    METHOD Prev()  VIRTUAL       
 
 ENDCLASS 
 
 //----------------------------------------------------------------------------//
 
 METHOD New( oOrm) CLASS OrmTable
 
    ::Orm  = oOrm
 
 return Self   
 
 //----------------------------------------------------------------------------//
 
 METHOD FieldPos( cFieldName ) CLASS OrmTable
 
    cFieldName = Upper( cFieldName )
 
 return AScan( ::aFields, { | aField | Upper( aField[ 1 ] ) == cFieldName } )   
 
 //----------------------------------------------------------------------------//
 CLASS MySQLTable FROM OrmTable
 
    DATA   hMyRes
    DATA   aRows
    DATA   nRow   INIT 1
 
    METHOD New( oOrm, hJson )
 
    METHOD Count()  
    METHOD FCount() INLINE Len( ::aFields )
    METHOD FieldName( n ) INLINE ::aFields[ n ][ 1 ]
 
    METHOD FieldGet( ncField )  INLINE ;
       ::aRows[ ::nRow ][ If( ValType( ncField ) == "C", ::FieldPos( ncField ), ncField ) ]
 
    METHOD FieldPut( ncField, uValue ) INLINE ;
       ::aRows[ ::nRow ][ If( ValType( ncField ) == "C", ::FieldPos( ncField ), ncField ) ] := uValue   
 
    METHOD FieldType( ncField ) INLINE ;
       ::aFields[ If( ValType( ncField ) == "C", ::FieldPos( ncField ), ncField ) ][ 2 ]  
 
    METHOD SeekPos(ncField, nValue) 

    METHOD Seek(ncField, nValue) 

    METHOD MultiSeek(acField,aValue)

    METHOD GoTo(nPos)   INLINE ::nRow := IF(::Count()<nPos,1,nPos)
    METHOD Next()   INLINE ::nRow++   
    METHOD Prev()   INLINE If( ::nRow > 1, ::nRow--,)   
    METHOD First()  INLINE ::nRow := 1
    METHOD Last()   INLINE ::nRow := Len( ::aRows )
 
 ENDCLASS   
 
 //----------------------------------------------------------------------------//

 METHOD SeekPos(ncField, nValue)
 
      LOCAL nVal:=::FieldPos( ncField )

 RETURN Ascan(::aRows,{|aVal| aVal[nVal]=nValue})

METHOD Count()

   LOCAL nCount:=0

   IF ::hMyRes = NIL
      nCount:=Len(::aRows)
   ELSE
      nCount:=mysql_num_rows( ::Orm:pLib, ::hMyRes )   
   ENDIF

RETURN nCount

 METHOD Seek(ncField, nValue)
   
   LOCAL nVal:=::FieldPos( ncField ), nPos

   IF (nPos := Ascan(::aRows,{|aVal| aVal[nVal]=nValue}))>0
      ::nRow := nPos
   ENDIF
 RETURN nPos

 METHOD MultiSeek(acField, aValue)
   
   LOCAL cCond:="{|aVal| ", nIt:=1, cVar, nPos:=0, nVal:=0
   
   FOR EACH cVar IN acField
      
      IF nIt > 1
         cCond+=" .AND. "
      ENDIF

      nVal:=::FieldPos( cVar )
      
      IF(nVal=0)
         RETURN nPos 
      ENDIF

      cCond += "aVal["+StrZero(nVal,2)+"]="+IF(ValType(aValue[nIt])="C" .OR. ValType(aValue[nIt])="D", "'"+aValue[nIt]+"'", hb_ValToStr(aValue[nIt]))

      nIt++
   NEXT 
   cCond+="}"

   IF (nPos := Ascan(::aRows,&(cCond)))>0
      ::nRow := nPos
   ENDIF
 RETURN nPos

 METHOD New( oOrm, hJson ) CLASS MySQLTable
 
    local n, m, hField, hRow

    ::Super:New( oOrm)

    IF hJson = NIL
 
      ::hMyRes = mysql_store_result( oOrm:pLib, oOrm:hConnection )
   
      if ::hMyRes == 0
         ? "mysql_store_results() failed"
      else
         ::aFields = Array( mysql_num_fields( oOrm:pLib, ::hMyRes ) )
         
         for n = 1 to Len( ::aFields )
            hField = mysql_fetch_field( oOrm:pLib, ::hMyRes )
            if hField != 0
               ::aFields[ n ] = Array( 4 )
               ::aFields[ n ][ 1 ] = PtrToStr( hField, 0 )
               do case
                  case AScan( { 253, 254, 12 }, PtrToUI( hField, hb_SysMyTypePos() ) ) != 0
                        ::aFields[ n ][ 2 ] = "C"
   
                  case AScan( { 1, 3, 4, 5, 8, 9, 246 }, PtrToUI( hField, hb_SysMyTypePos() ) ) != 0
                        ::aFields[ n ][ 2 ] = "N"
   
                  case AScan( { 10 }, PtrToUI( hField, hb_SysMyTypePos() ) ) != 0
                        ::aFields[ n ][ 2 ] = "D"
   
                  case AScan( { 250, 252 }, PtrToUI( hField, hb_SysMyTypePos() ) ) != 0
                        ::aFields[ n ][ 2 ] = "M"
               endcase 
            endif   
         next   
   
         ::aRows = Array( mysql_num_rows( oOrm:pLib, ::hMyRes ), ::FCount() )
   
         for n = 1 to Len( ::aRows )
            if ( hRow := mysql_fetch_row( oOrm:pLib, ::hMyRes ) ) != 0
               for m = 1 to ::FCount()
                  ::aRows[ n, m ] = PtrToStr( hRow, m - 1 )
               next
            endif
         next         
   
      endif
   ELSE
      ::aFields:= hJson['header']
      
      ::aRows  := hJson['body']

   ENDIF
    
 return Self   

 
 
 //----------------------------------------------------------------------------//
 
 function mysql_init(pLib)
 
 return hb_DynCall( { "mysql_init", pLib, hb_bitOr( hb_SysLong(),;
                    hb_SysCallConv() ) }, NULL )
 
 //----------------------------------------------------------------//
 
 function mysql_close( pLib, hMySQL )
 
 return hb_DynCall( { "mysql_close", pLib,;
                    hb_SysCallConv(), hb_SysLong() }, hMySQL )
 
 //----------------------------------------------------------------//
 
 function mysql_real_connect( pLib, cServer, cUserName, cPassword, cDataBaseName, nPort, hMySQL )
 
    if nPort == nil
       nPort = 3306
    endif   
 
 return hb_DynCall( { "mysql_real_connect", pLib, hb_bitOr( hb_SysLong(),;
                      hb_SysCallConv() ), hb_SysLong(),;
                      HB_DYN_CTYPE_CHAR_PTR, HB_DYN_CTYPE_CHAR_PTR, HB_DYN_CTYPE_CHAR_PTR, HB_DYN_CTYPE_CHAR_PTR,;
                      HB_DYN_CTYPE_LONG, HB_DYN_CTYPE_LONG, HB_DYN_CTYPE_LONG },;
                      hMySQL, cServer, cUserName, cPassword, cDataBaseName, nPort, 0, 0 )
                      
 //----------------------------------------------------------------//
 
 function mysql_query( pLib, hConnect, cQuery )   
 
 return hb_DynCall( { "mysql_query", pLib, hb_bitOr( HB_DYN_CTYPE_INT,;
                    hb_SysCallConv() ), hb_SysLong(), HB_DYN_CTYPE_CHAR_PTR },;
                    hConnect, cQuery )

 function mysql_real_query( pLib, hConnect, cQuery )   

 return hb_DynCall( { "mysql_real_query", pLib, hb_bitOr( HB_DYN_CTYPE_INT,;
                    hb_SysCallConv() ), hb_SysLong(), HB_DYN_CTYPE_CHAR_PTR, HB_DYN_CTYPE_LONG },;
                    hConnect, cQuery,Len(cQuery) )                    
 

//-----------------------------------------------------------------//
function mysql_real_escape_string_quote(pLib, hConnect, cQuery)
   
return hb_DynCall( { "mysql_real_escape_string", pLib, hb_bitOr( hb_SysLong(),;
hb_SysCallConv() ), hb_SysLong(), HB_DYN_CTYPE_CHAR_PTR, HB_DYN_CTYPE_CHAR_PTR, HB_DYN_CTYPE_LONG, HB_DYN_CTYPE_CHAR_PTR },;
hConnect, @cQuery, cQuery,  Len(cQuery), "\'")

function mysql_real_escape_string(pLib, hConnect, cQuery)
   
return hb_DynCall( { "mysql_real_escape_string", pLib, hb_bitOr( hb_SysLong(),;
hb_SysCallConv() ), hb_SysLong(), HB_DYN_CTYPE_CHAR_PTR, HB_DYN_CTYPE_CHAR_PTR, HB_DYN_CTYPE_LONG },;
hConnect, @cQuery, cQuery,  Len(cQuery))
 
 //----------------------------------------------------------------//
 
 function mysql_use_result( pLib, hMySQL )
 
 return hb_DynCall( { "mysql_use_result", pLib, hb_bitOr( hb_SysLong(),;
                    hb_SysCallConv() ), hb_SysLong() }, hMySQL )
 
 //----------------------------------------------------------------//
 
 function mysql_store_result( pLib, hMySQL )
 
 return hb_DynCall( { "mysql_store_result", pLib, hb_bitOr( hb_SysLong(),;
                    hb_SysCallConv() ), hb_SysLong() }, hMySQL )
 
 //----------------------------------------------------------------//
 
 function mysql_free_result( pLib, hMyRes) 
 
 return hb_DynCall( { "mysql_free_result", pLib,;
                    hb_SysCallConv(), hb_SysLong() }, hMyRes )
 
 //----------------------------------------------------------------//
 
 function mysql_fetch_row( pLib, hMyRes )
 
 return hb_DynCall( { "mysql_fetch_row", pLib, hb_bitOr( hb_SysLong(),;
                    hb_SysCallConv() ), hb_SysLong() }, hMyRes )
 
 //----------------------------------------------------------------//
 
 function mysql_num_rows( pLib, hMyRes )
 
 return hb_DynCall( { "mysql_num_rows", pLib, hb_bitOr( hb_SysLong(),;
                   hb_SysCallConv() ), hb_SysLong() }, hMyRes )
 
 //----------------------------------------------------------------//
 
 function mysql_num_fields( pLib, hMyRes )
 
 return hb_DynCall( { "mysql_num_fields", pLib, hb_bitOr( HB_DYN_CTYPE_LONG_UNSIGNED,;
                    hb_SysCallConv() ), hb_SysLong() }, hMyRes )
 
 //----------------------------------------------------------------//
 
 function mysql_fetch_field( pLib, hMyRes )
 
 return hb_DynCall( { "mysql_fetch_field", pLib, hb_bitOr( hb_SysLong(),;
                    hb_SysCallConv() ), hb_SysLong() }, hMyRes )
 
 //----------------------------------------------------------------//
 
 function mysql_get_server_info( pLib, hMySQL )
 
 return hb_DynCall( { "mysql_get_server_info", pLib, hb_bitOr( HB_DYN_CTYPE_CHAR_PTR,;
                    hb_SysCallConv() ), hb_SysLong() }, hMySql )
 
 //----------------------------------------------------------------//
 
 function mysql_error( pLib, hMySQL )
 
 return hb_DynCall( { "mysql_error", pLib, hb_bitOr( HB_DYN_CTYPE_CHAR_PTR,;
                    hb_SysCallConv() ), hb_SysLong() }, hMySql )
 
 //----------------------------------------------------------------//
 
 
 function hb_SysCallConv()
 
 return If( ! "Windows" $ OS(), HB_DYN_CALLCONV_CDECL, HB_DYN_CALLCONV_STDCALL )
 
 //----------------------------------------------------------------//
 
 function hb_SysMyTypePos()
 
 return If( hb_version( HB_VERSION_BITWIDTH ) == 64,;
        If( "Windows" $ OS(), 26, 28 ), 19 )   
 
 //----------------------------------------------------------------//
 