import 'package:flutter/foundation.dart';
import 'package:mutex/mutex.dart';
import 'package:stream_chat/stream_chat.dart';
import 'package:stream_chat_persistence/src/db/drift_chat_database.dart';
import 'package:stream_chat_persistence/stream_chat_persistence.dart';

///Provides direct access to drift DB to query channels in cache and delete channels by CID
class DirectDbAccess {
  // ignore: public_member_api_docs
  DirectDbAccess({
    Level logLevel = Level.WARNING,
    LogHandlerFunction? logHandlerFunction,
  }) : _logger = Logger.detached('💽')..level = logLevel {
    _logger.onRecord.listen(logHandlerFunction);
  }

  final Logger _logger;
  late final ConnectionMode _connectionMode;
  final _mutex = ReadWriteMutex();

  Future<T> _readProtected<T>(AsyncValueGetter<T> func) => _mutex.protectRead(func);

  @visibleForTesting
  DriftChatDatabase? db;

  DriftChatDatabase _defaultDatabaseProvider(
    String userId,
    ConnectionMode mode,
  ) =>
      SharedDB.constructDatabase(userId, connectionMode: mode);

  @override
  Future<void> connect(
    String userId,
    ConnectionMode connectionMode,
  ) async {
    if (db != null) {
      throw Exception(
        'An instance of StreamChatDatabase is already connected.\n'
        'disconnect the previous instance before connecting again.',
      );
    }
    _connectionMode = connectionMode;
    db = _defaultDatabaseProvider(userId, _connectionMode);
  }

  Future<void> disconnect({bool flush = false}) async => _mutex.protectWrite(() async {
        if (db != null) {
          if (flush) {
            await db!.flush();
          }
          await db!.disconnect();
          db = null;
        }
      });

  Future<List<ChannelModel>> getCacheChannels({
    Filter? filter,
  }) {
    _logger.info('get channel in cache');
    return _readProtected(
      () async {
        final channels = await db!.channelQueryDao.getChannels(
          filter: filter,
        );
        return channels;
      },
    );
  }

  Future<void> deleteChannels(List<String> cids) {
    _logger.info('delete channels in cache from cid');
    return _readProtected(() => db!.channelDao.deleteChannelByCids(cids));
  }
}
